// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2025, nuclEar_chaos, The Karbo developers

#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <queue>
#include <type_traits>
#include <utility>
#include <boost/asio.hpp>
#include <boost/coroutine/symmetric_coroutine.hpp>

namespace System {

  using coro_t = boost::coroutines::symmetric_coroutine<void>;

  struct NativeContextGroup;

  struct NativeContext {
    // Fiber replacement
    coro_t::call_type* coro{ nullptr };
    coro_t::yield_type* yield{ nullptr };

    bool interrupted{ false };
    bool inExecutionQueue{ false };
    NativeContext* next{ nullptr };
    NativeContextGroup* group{ nullptr };
    NativeContext* groupPrev{ nullptr };
    NativeContext* groupNext{ nullptr };
    std::function<void()> procedure;
    std::function<void()> interruptProcedure;
  };

  struct NativeContextGroup {
    NativeContext* firstContext{ nullptr };
    NativeContext* lastContext{ nullptr };
    NativeContext* firstWaiter{ nullptr };
    NativeContext* lastWaiter{ nullptr };
  };

  class Dispatcher {
  public:
    Dispatcher();
    Dispatcher(const Dispatcher&) = delete;
    ~Dispatcher();
    Dispatcher& operator=(const Dispatcher&) = delete;

    void clear();
    void dispatch();
    NativeContext* getCurrentContext() const;
    void interrupt();
    void interrupt(NativeContext* context);
    bool interrupted();
    void pushContext(NativeContext* context);
    void remoteSpawn(std::function<void()>&& procedure);
    void yield();

    // Timers (compatible API)
    void addTimer(uint64_t time, NativeContext* context);
    void interruptTimer(uint64_t time, NativeContext* context);

    // Legacy Windows API compat
    void* getCompletionPort() const { return nullptr; }

    // Context pool API (kept intact)
    NativeContext& getReusableContext();
    void pushReusableContext(NativeContext&);

    // Needed by TCP stack
    boost::asio::io_context& getIoContext() { return ioContext; }

    // Thread-safe, synchronous execution helper
    template<typename Func>
    auto execute(Func func) -> decltype(func());

  private:
    void spawn(std::function<void()>&& procedure);
    void contextProcedure(coro_t::yield_type& yield);
    void ensureIoContextReady();

    boost::asio::io_context ioContext;

    NativeContext mainContext;
    NativeContextGroup contextGroup;
    NativeContext* currentContext{ nullptr };
    NativeContext* firstResumingContext{ nullptr };
    NativeContext* lastResumingContext{ nullptr };
    NativeContext* firstReusableContext{ nullptr };
    size_t runningContextCount{ 0 };

    // Deadline (ms since steady_clock epoch) -> waiting context
    std::multimap<uint64_t, NativeContext*> timers;

    boost::asio::steady_timer wakeTimer{ ioContext };
    bool wakeArmed{ false };
    uint64_t wakeExpiryMs{ 0 };
  };

} // namespace System

template<typename Func>
auto System::Dispatcher::execute(Func func) -> decltype(func()) {
  using ResultType = decltype(func());

  // Create a promise/future pair
  auto promise = std::make_shared<std::promise<ResultType>>();
  auto future = promise->get_future();

  // The procedure to run on the Dispatcher thread
  auto procedure = [promise, func = std::move(func)]() mutable {
    try {
      if constexpr (std::is_void_v<ResultType>) {
        func();
        promise->set_value();
      }
      else {
        promise->set_value(func());
      }
    }
    catch (...) {
      promise->set_exception(std::current_exception());
    }
    };

  // Post the work
  this->remoteSpawn(std::move(procedure));

  // Block the calling thread until the result is ready
  return future.get();
}
