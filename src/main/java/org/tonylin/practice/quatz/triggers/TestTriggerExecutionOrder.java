package org.tonylin.practice.quatz.triggers;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tonylin.practice.quatz.SchedulerUtil;

public class TestTriggerExecutionOrder {
	private static Logger logger = LoggerFactory.getLogger(TestTriggerExecutionOrder.class);
	
	private static Trigger createTrigger(Date date, int priority, int index){
		String key = date.toString() + "-" + index;
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(key, "group").withPriority(priority).startAt(date)
				.build();
		logger.info("Add job trigger: {}", key);
		return trigger;
	}
	
	private static List<Trigger> createTriggers(int[][] timeOffsetPriorityArray){
		Calendar calendar = Calendar.getInstance();
		Date currentTime = calendar.getTime();
		
		int size = timeOffsetPriorityArray.length;
		
		List<Trigger> triggers  = new ArrayList<>();
		for( int i = 0; i < size ; i ++ ) {
			int timeOffset = timeOffsetPriorityArray[i][0];
			int priority = timeOffsetPriorityArray[i][1];
			
			calendar.setTime(currentTime);
			calendar.add(Calendar.SECOND, timeOffset);
			triggers.add(createTrigger(calendar.getTime(), priority, i));
		}
		
		return triggers;
	}
	
	private Scheduler scheduler = null;
	private CountDownLatch coutdownLatch;
	private List<JobKey> executionJobKeys = new CopyOnWriteArrayList<>();
	
	@Before
	public void setup() throws Exception {
		scheduler = SchedulerUtil.newScheduler("testScheduler", 1);
	}
	
	@After
	public void teardown(){
		SchedulerUtil.shutdown(scheduler);
		executionJobKeys.clear();
	}
	
	private void thenExecutionOrderShoudBe(int[] order, List<Trigger> triggers){
		assertEquals(order.length, executionJobKeys.size());
		int i = 0;
		for( JobKey jobKey : executionJobKeys ) {
			assertEquals(jobKey, triggers.get(order[i]).getJobKey());
			i++;
		}
	}
	
	private List<Trigger> givenScheduleJobs(int[][] timeOffsetPriorityArray) throws Exception{
		coutdownLatch = new CountDownLatch(timeOffsetPriorityArray.length);
		List<Trigger> triggers = createTriggers(timeOffsetPriorityArray);
		
		for( Trigger trigger :  triggers) {
			JobDetail jobDetail = JobBuilder.newJob(TestJob.class)
					.withIdentity(trigger.getKey().getName(), "group").build();
			jobDetail.getJobDataMap().put(CountDownLatch.class.getName(), coutdownLatch);
			jobDetail.getJobDataMap().put(List.class.getName(), executionJobKeys);
			scheduler.scheduleJob(jobDetail, trigger);
		}
		return triggers;
	}

	/**
	 * 逾時過久的工作，如果優先度相同，
	 * 會以最早的工作優先執行。
	 * 
	 * @throws Exception
	 */
	@Test
	public void someDelayJobWithSamePriority() throws Exception {
		// given
		int[][] timeOffsetPriorityArray = {
				{-10, 5},
				{-20, 5},
				{   0, 5},
				{   0, 5},
				{   1, 5},
				{  10, 5}
		};
		List<Trigger> triggers = givenScheduleJobs(timeOffsetPriorityArray);
		
		// when
		scheduler.start();
		
		// then
		coutdownLatch.await(30, TimeUnit.SECONDS);
		thenExecutionOrderShoudBe(new int[]{2, 3, 1, 0, 4, 5}, triggers);
	}
	
	/**
	 * 逾時過久的工作，如果優先度不同，
	 * 會以優先度高的先執行。
	 * 
	 * @throws Exception
	 */
	@Test
	public void someDelayJobWithDiffPriority() throws Exception {
		// given
		int[][] timeOffsetPriorityArray = {
				{-20, 6},
				{-10, 10},
				{   0, 5},
				{   0, 5},
				{   1, 8},
				{  10, 5}
		};
		List<Trigger> triggers = givenScheduleJobs(timeOffsetPriorityArray);

		// when
		scheduler.start();
		
		// then
		coutdownLatch.await(30, TimeUnit.SECONDS);
		thenExecutionOrderShoudBe(new int[]{2, 3, 1, 0, 4, 5}, triggers);
	}
	
	/**
	 * 過期5秒內會優先排，會根據工作時間先後順序；
	 * 過期超過5秒的，會根據工作時間先後順序。
	 * 
	 * @throws Exception
	 */
	@Test
	public void allDelayJobsWithSamePriority() throws Exception {
		// given
		int[][] timeOffsetPriorityArray = {
				{-50, 5},
				{-40, 5},
				{-5, 5},
				{-2, 5},
				{-3, 5}
		};
		List<Trigger> triggers = givenScheduleJobs(timeOffsetPriorityArray);

		// when
		scheduler.start();
		
		// then
		coutdownLatch.await(30, TimeUnit.SECONDS);
		thenExecutionOrderShoudBe(new int[]{4, 3, 0, 1, 2}, triggers);
	}
	
	/**
	 * 5秒以內的工作不管優先度如何，
	 * 都會以最早的工作為優先。
	 * (除非時間相同)
	 * 
	 * @throws Exception
	 */
	@Test
	public void lessThenFiveSeoncdsJobs() throws Exception {
		// given
		int[][] timeOffsetPriorityArray = {
				{2, 11},
				{1, 10},
				{0, 9},
				{-1, 8},
				{-2, 7}
		};
		List<Trigger> triggers = givenScheduleJobs(timeOffsetPriorityArray);

		// when
		scheduler.start();
		
		// then
		coutdownLatch.await(30, TimeUnit.SECONDS);
		thenExecutionOrderShoudBe(new int[]{4, 3, 2, 1, 0}, triggers);
	}
}
