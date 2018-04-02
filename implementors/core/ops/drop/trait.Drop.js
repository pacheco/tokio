(function() {var implementors = {};
implementors["tokio"] = ["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio/runtime/struct.Runtime.html\" title=\"struct tokio::runtime::Runtime\">Runtime</a>",];
implementors["tokio_executor"] = ["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio_executor/struct.Enter.html\" title=\"struct tokio_executor::Enter\">Enter</a>",];
implementors["tokio_reactor"] = ["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio_reactor/struct.Background.html\" title=\"struct tokio_reactor::Background\">Background</a>","impl&lt;E:&nbsp;<a class=\"trait\" href=\"https://docs.rs/mio/0.6.14/mio/event_imp/trait.Evented.html\" title=\"trait mio::event_imp::Evented\">Evented</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio_reactor/struct.PollEvented.html\" title=\"struct tokio_reactor::PollEvented\">PollEvented</a>&lt;E&gt;",];
implementors["tokio_threadpool"] = ["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio_threadpool/struct.ThreadPool.html\" title=\"struct tokio_threadpool::ThreadPool\">ThreadPool</a>","impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio_threadpool/struct.Worker.html\" title=\"struct tokio_threadpool::Worker\">Worker</a>",];
implementors["tokio_timer"] = ["impl&lt;T, N&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/drop/trait.Drop.html\" title=\"trait core::ops::drop::Drop\">Drop</a> for <a class=\"struct\" href=\"tokio_timer/timer/struct.Timer.html\" title=\"struct tokio_timer::timer::Timer\">Timer</a>&lt;T, N&gt;",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
