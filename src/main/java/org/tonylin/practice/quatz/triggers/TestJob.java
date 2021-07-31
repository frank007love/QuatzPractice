package org.tonylin.practice.quatz.triggers;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class  TestJob implements Job {
	private static Logger logger = LoggerFactory.getLogger(TestJob.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobKey jobKey = context.getTrigger().getJobKey();
		logger.info("Execute: {}", jobKey);
		CountDownLatch countdownLatch = (CountDownLatch)context.getJobDetail().getJobDataMap().get(CountDownLatch.class.getName());
		List<JobKey> sharedJobKeys =  (List<JobKey>)context.getJobDetail().getJobDataMap().get(List.class.getName());
		if(sharedJobKeys!=null)
			sharedJobKeys.add(jobKey);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			logger.error("", e);
		} finally {
			if( countdownLatch!=null ) {
				logger.debug("countDown");
				countdownLatch.countDown();
			}
		}
	}
}
