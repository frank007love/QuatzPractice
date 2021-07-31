package org.tonylin.practice.quatz.triggers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tonylin.practice.quatz.SchedulerUtil;

public class TestCronTrigger {
	private static Logger logger = LoggerFactory.getLogger(TestCronTrigger.class);
	
	private Scheduler scheduler = null;
	private CountDownLatch coutdownLatch;
	
	@Before
	public void setup() throws Exception {
		scheduler = SchedulerUtil.newScheduler("testScheduler", 1);
	}
	
	@After
	public void teardown(){
		SchedulerUtil.shutdown(scheduler);
	}
	
	@Test
	public void helloWorld() throws Exception  {
		// given
		Trigger trigger = TriggerBuilder.newTrigger()
	    		.withIdentity("trigger1", "group1")
	    		.withSchedule(CronScheduleBuilder.cronSchedule("0/2 * * * * ?"))
	    		.build();
		
		coutdownLatch = new CountDownLatch(2);
		JobDetail jobDetail = JobBuilder.newJob(TestJob.class)
				.withIdentity(trigger.getKey().getName(), "group").build();
		jobDetail.getJobDataMap().put(CountDownLatch.class.getName(), coutdownLatch);
		
		scheduler.scheduleJob(jobDetail, trigger);
		assertEquals(2, coutdownLatch.getCount());
		
		// when
		scheduler.start();
		
		// then
		coutdownLatch.await(5, TimeUnit.SECONDS);
		assertEquals(0, coutdownLatch.getCount());
	}
}
