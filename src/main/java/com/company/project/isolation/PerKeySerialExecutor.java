package com.company.project.isolation;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PerKeySerialExecutor
 * - 同一 key（如 orderId）上的任务严格串行
 * - 不同 key 之间可并行（由底层共享线程池并发执行）
 */
public class PerKeySerialExecutor {

    private static class SerialQueue implements Runnable {
        private final Deque<Runnable> tasks = new ArrayDeque<>();
        private final Executor backend;
        private volatile boolean running = false;

        SerialQueue(Executor backend) {
            this.backend = backend;
        }

        synchronized void execute(Runnable task) {
            tasks.addLast(task);
            if (!running) {
                running = true;
                backend.execute(this);
            }
        }

        @Override
        public void run() {
            for (;;) {
                final Runnable task;
                synchronized (this) {
                    task = tasks.pollFirst();
                    if (task == null) {
                        running = false;
                        return;
                    }
                }
                try {
                    task.run();
                } catch (Throwable t) {
                    // swallow to continue next tasks
                }
            }
        }

        synchronized boolean isEmpty() {
            return tasks.isEmpty() && running == false;
        }
    }

    private final Map<String, SerialQueue> keyToQueue = new ConcurrentHashMap<>();
    private final Executor sharedPool;

    public PerKeySerialExecutor(int parallelism, String threadNamePrefix) {
        this.sharedPool = Executors.newFixedThreadPool(parallelism, new ThreadFactory() {
            private final AtomicInteger idx = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(threadNamePrefix + idx.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void execute(String key, Runnable task) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(task, "task must not be null");
        SerialQueue queue = keyToQueue.computeIfAbsent(key, k -> new SerialQueue(sharedPool));
        queue.execute(task);
    }

    /**
     * 清理空队列，避免 key 无限增长（可定时调用）。
     */
    public void cleanupIdleKeys() {
        keyToQueue.entrySet().removeIf(e -> e.getValue().isEmpty());
    }
}
