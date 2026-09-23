//! Named fault-injection points, compiled out unless the `failpoints`
//! feature is on.
//!
//! # Why named points rather than random chaos
//!
//! These exist to be a merge gate, so every failure has to be reproducible
//! and regressable: a test that kills the process at a random moment is red
//! once and green the next run, and nobody can rerun the case it found. A
//! named point makes each scenario a specific test with a specific command,
//! so a failure is a fact rather than a rumour.
//!
//! # Why here and not in an object-store layer
//!
//! An opendal layer sees "a write happened" and cannot tell the staged data
//! file from the lease file, which is exactly the distinction most of these
//! scenarios turn on. [`crate::staging::StagingStore`] is a narrow waist --
//! every object-store call in the SDK goes through its handful of methods --
//! so a point per method is both more precise and strictly more capable.
//! (opendal's own `ChaosLayer` is moot regardless: it injects read
//! operations only, and this SDK's hot path is writes.)
//!
//! # Cost when the feature is off
//!
//! Nothing: `fail` is an optional dependency, both macros below expand to
//! nothing, and the published crate is built without the feature. The
//! release artifact carries no extra dependency and no runtime check.
//!
//! # Activating a point
//!
//! Through `failpoints_testing::Armed` (or `fail::cfg` underneath it), and
//! only that way.
//!
//! **Not through the `FAILPOINTS` environment variable**, even though `fail`
//! documents it: that variable is read by `fail::FailScenario::setup()`
//! alone, which this crate never calls -- `fail_point!` itself only consults
//! the registry. Setting `FAILPOINTS=...` here therefore does nothing, and
//! the run that follows looks exactly like a run that survived the fault.
//! For a crate whose subject is "a gate must not pass by not running", that
//! is the worst available failure mode, so the variable is deliberately left
//! unsupported rather than half-supported. (Adopting it would mean owning
//! `FailScenario`'s lifetime too: its `Drop` clears the whole registry,
//! which would fight `Armed`.)
//!
//! Two kinds of point, and the action table differs between them:
//!
//! | macro | accepts | effect |
//! |---|---|---|
//! | `failpoint!` | `return` | the call fails with `Error::Injected` |
//! | `failpoint!` | `return(abort)` | the process dies on the spot |
//! | `crash_point!` | `return(abort)` | the process dies on the spot |
//! | `delay_point!` | `return(<millis>)` | holds that moment open for `millis`, asynchronously |
//! | any | `off` | the point is inert (the default) |
//!
//! A `crash_point!` armed with anything but `abort` panics rather than
//! quietly aborting: these points sit in code that could return an error,
//! so a silent abort would be indistinguishable from the injected-error
//! behaviour of the `failpoint!` next to it. `staging::put::before` and
//! `staging::put::after` are two lines apart inside `put()`.
//!
//! `fail`'s own `sleep`, `pause` and `panic` work on `failpoint!` too; they
//! need no help from us.
//!
//! # The one rule for tests
//!
//! **`fail`'s configuration is global process state.** Two tests activating
//! points at the same time see each other's, so every suite that touches a
//! failpoint must run with `--test-threads=1`. The e2e job already does;
//! `tests/README.md` says why, because the failure mode of getting this
//! wrong is a random red rather than an obvious one.

/// What an activated point does, given the action's argument.
///
/// Returns `Err` for every argument except `abort`, which never returns.
/// Generic in the success type so one helper serves every method, none of
/// which can succeed once the point has fired.
#[cfg(feature = "failpoints")]
pub(crate) fn act<T>(name: &str, arg: Option<String>) -> crate::error::Result<T> {
    match arg.as_deref() {
        // The crash scenarios: abort rather than panic, because a panic
        // unwinds and runs destructors, which is not what a killed process
        // does -- and the recovery path under test is the one that has to
        // cope with a writer that stopped mid-operation.
        Some("abort") => std::process::abort(),
        other => Err(crate::error::Error::Injected {
            point: name.to_string(),
            arg: other.map(str::to_string),
        }),
    }
}

/// Arming points from a test, without `fail` becoming a dev-dependency of
/// the crate (it would then be compiled by anyone running `cargo test` here,
/// feature or not).
#[cfg(feature = "failpoints")]
pub mod testing {
    /// An armed point, disarmed when this value is dropped.
    ///
    /// A guard rather than a pair of calls because `fail`'s configuration is
    /// process-global: a test that panicked between arming and disarming
    /// would leave the point armed for every test after it, and the report
    /// would blame the wrong one. Drop runs during unwinding, so the guard
    /// holds even then.
    #[must_use = "the point is disarmed as soon as this is dropped"]
    pub struct Armed(String);

    impl Armed {
        /// `action` is `fail`'s own syntax: `return`, `return(abort)`,
        /// `50%return`, `sleep(100)`, and so on.
        pub fn new(point: &str, action: &str) -> Self {
            fail::cfg(point, action)
                .unwrap_or_else(|e| panic!("arm failpoint `{point}` as `{action}`: {e}"));
            Self(point.to_string())
        }
    }

    impl Drop for Armed {
        fn drop(&mut self) {
            fail::remove(&self.0);
        }
    }
}

/// A fault-injection point inside a function returning [`crate::error::Result`].
/// Expands to nothing without the `failpoints` feature.
macro_rules! failpoint {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        {
            fail::fail_point!($name, |arg| {
                return $crate::failpoints::act($name, arg);
            });
        }
    };
}

/// A point that can only DELAY, to turn a race into a test.
///
/// A race between two tasks is a lottery until one of them can be held
/// still inside its window; this macro is where it is held. Armed as
/// `return(<millis>)`, it sleeps that long and then continues as if it were
/// not there. Nothing is injected and nothing fails -- the fault under test
/// is what the OTHER task does meanwhile.
///
/// `return(<millis>)`, not `fail`'s own `sleep(<millis>)`: that one is a
/// `std::thread::sleep`, which parks the runtime thread it runs on. In a
/// current-thread runtime -- every `#[tokio::test]` by default -- that
/// parks the test body too, including the call that was supposed to race
/// this window, and the race silently never happens. Taking the argument
/// through `return` lets us sleep asynchronously instead. Any other
/// argument is rejected loudly, so a point armed the wrong way cannot pass
/// as one that was never reached.
///
/// Only usable in an `async` context. Expands to nothing without the
/// `failpoints` feature.
macro_rules! delay_point {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        {
            if let Some(arg) = fail::eval($name, |arg: Option<String>| arg) {
                let millis: u64 = match arg.as_deref().map(str::parse) {
                    Some(Ok(ms)) => ms,
                    _ => panic!(
                        "failpoint `{}` is a delay point: arm it as `return(<millis>)`, got {:?}",
                        $name, arg
                    ),
                };
                tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
            }
        }
    };
}

/// A point that models a KILLED process rather than a failed call.
///
/// Not "a function with no error to return" -- both of its uses return
/// `Result<()>`. The distinction is what is being modelled: these points
/// exist so a test can stop the process where a crash would stop it, with
/// nothing unwound and nothing released, which is a different scenario from
/// the call failing.
///
/// Rejects any action but `abort`, loudly. It would otherwise abort on a
/// plain `return` as well, and a test arming `return` would see the process
/// vanish with no libtest output at all -- especially confusing where a
/// `failpoint!` and a `crash_point!` sit two lines apart, as they do in
/// `StagingStore::put`.
///
/// Expands to nothing without the `failpoints` feature.
macro_rules! crash_point {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        {
            fail::fail_point!($name, |arg: Option<String>| {
                match arg.as_deref() {
                    Some("abort") => std::process::abort(),
                    other => panic!(
                        "failpoint `{}` is a crash point: it only accepts `return(abort)`, got {:?}",
                        $name, other
                    ),
                }
            });
        }
    };
}
