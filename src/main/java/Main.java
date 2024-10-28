import java.util.UUID;
import java.util.concurrent.*;

public class Main {

    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        <T> Future<T> submitTask(Task<T> task);
    }

    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public static class TaskExecutorService implements TaskExecutor {
        private final ExecutorService executorService;
        private final ConcurrentHashMap<UUID, Semaphore> groupLocks;
        private final BlockingQueue<Runnable> taskQueue;

        public TaskExecutorService(int maxConcurrency) {
            this.executorService = Executors.newFixedThreadPool(maxConcurrency);
            this.groupLocks = new ConcurrentHashMap<>();
            this.taskQueue = new LinkedBlockingQueue<>();
            startTaskProcessor();
        }

        private void startTaskProcessor() {
            executorService.submit(() -> {
                while (true) {
                    try {
                        Runnable task = taskQueue.take(); // Wait for tasks to process
                        task.run(); // Execute the task
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break; // Stop processing on interrupt
                    }
                }
            });
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            UUID groupUUID = task.taskGroup().groupUUID();
            groupLocks.putIfAbsent(groupUUID, new Semaphore(1)); // Create semaphore for group if not exists

            CompletableFuture<T> future = new CompletableFuture<>();

            Runnable runnableTask = () -> {
                try {
                    groupLocks.get(groupUUID).acquire(); // Wait for group lock
                    T result = task.taskAction().call(); // Execute task
                    future.complete(result); // Set result
                } catch (Exception e) {
                    future.completeExceptionally(e); // Handle exceptions
                } finally {
                    groupLocks.get(groupUUID).release(); // Release lock
                }
            };

            taskQueue.offer(runnableTask); // Add task to queue
            return future;
        }

        public void shutdown() {
            executorService.shutdown(); // Shut down executor service
        }
    }

    public static void main(String[] args) {
        TaskExecutorService executorService = new TaskExecutorService(3);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        Callable<String> taskAction1 = () -> {
            Thread.sleep(1000);
            System.out.println("Task 1 completed");
            return "Result of Task 1";
        };

        Callable<String> taskAction2 = () -> {
            Thread.sleep(500);
            System.out.println("Task 2 completed");
            return "Result of Task 2";
        };

        Callable<String> taskAction3 = () -> {
            Thread.sleep(700);
            System.out.println("Task 3 completed");
            return "Result of Task 3";
        };

        // Submitting tasks to the executor service
        Future<String> future1 = executorService.submitTask(new Task<>(UUID.randomUUID(), group1, TaskType.READ, taskAction1));
        Future<String> future2 = executorService.submitTask(new Task<>(UUID.randomUUID(), group1, TaskType.WRITE, taskAction2));
        Future<String> future3 = executorService.submitTask(new Task<>(UUID.randomUUID(), group2, TaskType.READ, taskAction3));

        try {
            // Getting results from futures
            System.out.println("Future 1: " + future1.get());
            System.out.println("Future 2: " + future2.get());
            System.out.println("Future 3: " + future3.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}