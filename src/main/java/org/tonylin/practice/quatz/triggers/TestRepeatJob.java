package org.tonylin.practice.quatz.triggers;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.tonylin.practice.quatz.SchedulerUtil;

public class TestRepeatJob {

	private Scheduler scheduler = null;
	
	@Before
	public void setup() throws Exception {
		scheduler = SchedulerUtil.newScheduler("testScheduler", 10);
		counter = new AtomicInteger(0);
	}
	
	@After
	public void teardown(){
		SchedulerUtil.shutdown(scheduler);
	}
	
	private static AtomicInteger counter;
	
	public static class RepeatJob implements Job {

		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException {
			counter.incrementAndGet();
		}
	}
	
	@Test
	public void testRepeat() throws Exception {
		JobDetail jobDetail = JobBuilder.newJob(RepeatJob.class).withIdentity("jobdetail", "group1").build();

		Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger", "group1").startNow()
				.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(2).repeatForever()).build();

		
		scheduler.scheduleJob(jobDetail, trigger);
		scheduler.start();
		
		
		Thread.sleep(11*1000);
		assertEquals(5+1, counter.get());
	}
	
	@DisallowConcurrentExecution
	public static class DisallowConcurrentExecutionJob implements Job {

		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException {
			counter.incrementAndGet();
			try {
				Thread.sleep(10*1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new JobExecutionException(e);
			}
		}
	}
	
	@Test
	public void testDisallowConcurrentExecution() throws Exception {
		JobDetail jobDetail = JobBuilder.newJob(DisallowConcurrentExecutionJob.class).withIdentity("jobdetail", "group1").build();

		Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger", "group1").startNow()
				.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(2).repeatForever()).build();

		
		scheduler.scheduleJob(jobDetail, trigger);
		scheduler.start();
		
		Thread.sleep(4*1000);
		assertEquals(1, counter.get());
	}
}
