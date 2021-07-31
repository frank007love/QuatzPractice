package org.tonylin.practice.quatz;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerUtil {
	private static Logger logger = LoggerFactory.getLogger(SchedulerUtil.class);

	public static Scheduler newScheduler(String aSchedulerName, int aPoolSize) throws SchedulerException {

		StdSchedulerFactory factory = new StdSchedulerFactory();
		Properties props = new Properties();
		props.put(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, aSchedulerName);
		props.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
		props.put(StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadCount", String.valueOf(aPoolSize));
		props.put(StdSchedulerFactory.PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN, "true");
		factory.initialize(props);
		return factory.getScheduler();
	}
	public static void shutdown(Scheduler aScheduler) {
		try {
			if (null != aScheduler)
				aScheduler.shutdown();
		} catch (SchedulerException e) {
			logger.error("Fails to shutdown Quartz scheduler.", e);
		}
	}
	
	public static boolean isJobCurrentlyExecuted(Scheduler scheduler, JobKey jobKey) throws SchedulerException{
		
		List<JobExecutionContext> jobContexts = scheduler.getCurrentlyExecutingJobs();
		
		List<JobExecutionContext> result =jobContexts.stream().filter(j->j.getJobDetail().getKey().equals(jobKey)).collect(Collectors.toList());
		
		if(result.size()>0) { 
			logger.warn("this Job {} is executing currently, CurrentlyExecutingJobs size is {}", jobKey, scheduler.getCurrentlyExecutingJobs().size());
			logger.warn("the result is {}", result);
			return true;
		}
		return false;
	}
}
