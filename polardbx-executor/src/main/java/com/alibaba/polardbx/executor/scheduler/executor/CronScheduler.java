package com.alibaba.polardbx.executor.scheduler.executor;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class CronScheduler {

    private final Map<Runnable, String> tasks = new HashMap<>();
    private final CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
    private final CronParser parser = new CronParser(cronDefinition);
    private final CronDescriptor descriptor = CronDescriptor.instance();

    public void addTask(Runnable task, String cronExpression) {
        tasks.put(task, cronExpression);
    }

    public void start() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                ZonedDateTime now = ZonedDateTime.now();
                for (Map.Entry<Runnable, String> entry : tasks.entrySet()) {
                    Runnable task = entry.getKey();
                    String cronExpression = entry.getValue();

                    try {
                        Cron cron = parser.parse(cronExpression);
                        ExecutionTime executionTime = ExecutionTime.forCron(cron);
                        Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(now); // Get next execution time

                        if (!nextExecution.isPresent()) {
                            // Handle the case where there's no next execution time.
                            System.err.println("No next execution time found.");
                        }

                        // Check if the next execution time is within the next minute
                        if (nextExecution.isPresent() && nextExecution.get().isBefore(now.plusMinutes(1))) {
                            // Use nextRun
                            task.run();
                        }
                        System.out.println("Next execution: " + nextExecution.get());

                    } catch (IllegalArgumentException e) {
                        System.err.println("Invalid cron expression: " + cronExpression + ". Error: " + e.getMessage());
                    }
                }
            }
        }, 0, TimeUnit.MINUTES.toMillis(1));  // Schedule every minute
    }


    public static void main(String[] args) throws InterruptedException {
        CronScheduler scheduler = new CronScheduler();

        // Example tasks
        Runnable task1 = () -> System.out.println("Task 1 executed at " + ZonedDateTime.now());
        Runnable task2 = () -> System.out.println("Task 2 executed at " + ZonedDateTime.now());


        // Add tasks with cron expressions
        scheduler.addTask(task1, "*/1 * * * *");  // Every minute
        scheduler.addTask(task2, "15 * * * *"); // Every hour at minute 15

        scheduler.start();

        // Keep the main thread alive (for demonstration)
        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
    }
}

