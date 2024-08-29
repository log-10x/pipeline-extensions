package com.log10x.ext.edge.output.proc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility output stream used by {@link ProcOutputStream} to launch
 * and write data in a spawned sub-process' stdin pipe.
 */
public class ProcOutputStreamImpl extends OutputStream {
		
	private static final Logger logger = LogManager.getLogger(ProcOutputStreamImpl.class);
	
	protected class ProcOutputLogTask implements Runnable {

		private final BufferedReader reader;
		
		private final boolean stderr;

		protected ProcOutputLogTask(BufferedReader reader, boolean stderr) {
			this.reader = reader;
			this.stderr = stderr;			
		}
		
		@Override
		public void run() {
			
			Thread.currentThread().setName(
				(this.stderr ? "stderr: " : "stdout : ") + options.command());
				
			try {
				
				String line;

				while ((!completed) && ((line = reader.readLine()) != null)) {
					
					if (this.stderr) {
						
						logger.error(line);
						
					} else if (logger.isInfoEnabled()) {
						
						logger.info(line);
					}
										
					currReadLines.incrementAndGet();
				}
				
			} catch (Exception e) {
				
				if (!closed) {
					logger.error("error reading line: " + currReadLines, e);
				}
			}
			
			if (logger.isDebugEnabled()) {
				
				logger.debug("read: " + currReadLines + " from process " + 
					((this.stderr) ? "error" : "output"));
			}
		}
		
	}
		
	public final ProcOutputStreamOptions options;
	
	private final long destroyWaitTime;
		
	private final AtomicLong currReadLines;

	private ExecutorService executorService;

	private volatile Process process;
	
	private OutputStream out;
		
	private boolean closed;
	
	private volatile boolean completed;

	public ProcOutputStreamImpl(ProcOutputStreamOptions options, long destroyWaitTime) {
				
		if ((options.procOutCommand == null) ||
			(options.procOutCommand.isBlank())) {
			
			throw new IllegalArgumentException("no command");
		}
		
		this.options = options;
		this.destroyWaitTime = destroyWaitTime;
		
		this.currReadLines = new AtomicLong();
		
		if ((!options.procOutLazyLaunch) &&
			(!this.startIfNeeded())) {
			
			throw new IllegalStateException("could not launch: " + options.command());
		}
	}

	public boolean isAlive() {
		
		return
			(this.process != null) && 
			(process.isAlive());	
	}
	
	private boolean startIfNeeded() {
		
		if (this.closed) {
			return false;
		}
		
		if (this.process != null) {
			return true;
		}
		
		synchronized (this) {
			
			if (this.process != null) {
				return true;
			}
	
			List<String> command = new ArrayList<>(options.procOutArgs.size() + 1);
			
			command.add(options.procOutCommand);
			command.addAll(options.procOutArgs);
			
			if (logger.isDebugEnabled()) {
				logger.debug("launching: " + command);
			}
			
			ProcessBuilder builder = new ProcessBuilder(command);
			
			try {
			
				Process createdProcess = builder.start();	
				this.out = createdProcess.getOutputStream();
				
				this.executorService = Executors.newFixedThreadPool(2);

				executorService.submit(new ProcOutputLogTask(createdProcess.errorReader(), true));
				executorService.submit(new ProcOutputLogTask(createdProcess.inputReader(), false));
	
				this.process = createdProcess;
				
				return true;
				
			} catch (Exception e) {
			
				this.closed = true;
				logger.error("error launching: " + command, e);
				
				return false;
				
			} finally {
				
				if (this.executorService != null) {
					executorService.shutdown();
				}
			}
		}
	}

	@Override
	public synchronized void write(byte[] b, int off, int len) throws IOException {
		
		if (this.startIfNeeded()) {			
			out.write(b, off, len);
		}
	}
	
	@Override
	public synchronized void write(int b) throws IOException {
		
		if (this.startIfNeeded()) {		
			out.write(b);
		}
	}
	
	@Override
	public synchronized void flush() throws IOException {
			
		if (this.process != null) {
			out.flush();
		}
	}
	
	
	@Override
	public synchronized void close() throws IOException {
		
		if (this.process == null) {
			
			if (logger.isDebugEnabled()) {
				logger.debug("closing, no process started");
			}
			
			return;
		}
		
		if (this.closed) {
			
			if (logger.isDebugEnabled()) {
				logger.debug("closing, already closed");
			}
			
			return;
		}
		
		try	{
			
			this.flush();
			
			this.closed = true;
	
			switch (options.procOutDestroyMode) {
			
			case FORCIBLE:
			
				if (logger.isDebugEnabled()) {
					logger.debug("destroying forcibly");
				}
				
				process.destroyForcibly();
				
				break;
				
			case GREACEFUL:
			case CACHED:

				if (logger.isDebugEnabled()) {
					logger.debug("destroying gracefully, wait:" + this.destroyWaitTime);
				}
				
				process.destroy();
				
				boolean timeout;
				
				if (this.destroyWaitTime > 0) {
					
					timeout = process.waitFor(this.destroyWaitTime,
						TimeUnit.MILLISECONDS);
					
				} else {
					
					timeout = false;
					process.waitFor();
				}
								
				if (logger.isDebugEnabled()) {
					
					int exitValue = process.exitValue();
					
					logger.error("destroyed. timeout: " + 
						timeout + " exitcode: " + exitValue);
				}
				
				break;	
			}
			
			this.completed = true;
			
		} catch (Exception e) {
			
			logger.error("error closing: " + options.command(), e);
		}		
	}
}