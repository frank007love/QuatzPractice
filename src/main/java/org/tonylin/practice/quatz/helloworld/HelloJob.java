package org.tonylin.practice.quatz.helloworld;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloJob implements Job {
	private static Logger logger = LoggerFactory.getLogger(HelloJob.class);
	// ref: https://www.jianshu.com/p/baf4537f2df0
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		logger.info("Hello World!");
	}

}
