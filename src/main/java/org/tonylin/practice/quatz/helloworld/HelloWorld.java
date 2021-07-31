package org.tonylin.practice.quatz.helloworld;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorld {
	private static Logger logger = LoggerFactory.getLogger(HelloWorld.class);
	/**
	 * ToDo
	 * 1. ThreadPool configuration.
	 * 2. Identify the time rule of getting job.
	 */
	
	public static void main(String[] args)  throws Exception  {
		// 创建一个JobDetail
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity("jobdetail", "group1").build();
        // 创建一个Trigger，每2秒执行一次
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger", "group1")
                .startNow().withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().
                                withIntervalInSeconds(2).repeatForever())
                .build();
        // 创建一个scheduler
        SchedulerFactory factory = new StdSchedulerFactory();
        Scheduler scheduler = factory.getScheduler();
        scheduler.scheduleJob(jobDetail, trigger);
        
        logger.info("Start Hello World!");
        // 启动schduler
        scheduler.start();
	}
	
}
