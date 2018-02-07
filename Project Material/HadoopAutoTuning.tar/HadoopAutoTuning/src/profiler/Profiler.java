
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

/**
 * This class provides static methods for enabling and performing MapReduce job
 * profiling.
 * 
 * @author 
 */
public class Profiler {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Public constants
	public static final String PROFILER_BTRACE_DIR = "opensimplex.profiler.btrace.dir";
	public static final String PROFILER_CLUSTER_NAME = "opensimplex.profiler.cluster.name";
	public static final String PROFILER_OUTPUT_DIR = "opensimplex.profiler.output.dir";
	public static final String PROFILER_RETAIN_TASK_PROFS = "opensimplex.profiler.retain.task.profiles";
	public static final String PROFILER_COLLECT_TRANSFERS = "opensimplex.profiler.collect.data.transfers";
	public static final String PROFILER_SAMPLING_MODE = "opensimplex.profiler.sampling.mode";
	public static final String PROFILER_SAMPLING_FRACTION = "opensimplex.profiler.sampling.fraction";

	private static final Log LOG = LogFactory.getLog(Profiler.class);

	private static final Pattern JOB_PATTERN = Pattern
			.compile(".*(job_[0-9]+_[0-9]+).*");

	private static final Pattern TRANSFERS_PATTERN = Pattern
			.compile(".*(Shuffling|Read|Failed).*");

	private static String OLD_MAPPER_CLASS = "mapred.mapper.class";
	private static String OLD_REDUCER_CLASS = "mapred.reducer.class";

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * This method will enable dynamic profiling using the BTraceTaskProfile
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "opensimplex.profiler.btrace.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	public static boolean enableExecutionProfiling(Configuration conf) {

		if (enableProfiling(conf)) {
			conf.set("mapred.task.profile.params", "-javaagent:"
					+ "${opensimplex.profiler.btrace.dir}/btrace-agent.jar="
					+ "dumpClasses=false,debug=false,"
					+ "unsafe=true,probeDescPath=.,noServer=true,"
					+ "script=${opensimplex.profiler.btrace.dir}/"
					+ "BTraceTaskProfile.class,scriptOutputFile=%s");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method will enable dynamic profiling using the BTraceTaskMemProfile
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "opensimplex.profiler.btrace.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	public static boolean enableMemoryProfiling(Configuration conf) {

		if (enableProfiling(conf)) {
			conf.set("mapred.task.profile.params", "-javaagent:"
					+ "${opensimplex.profiler.btrace.dir}/btrace-agent.jar="
					+ "dumpClasses=false,debug=false,"
					+ "unsafe=true,probeDescPath=.,noServer=true,"
					+ "script=${opensimplex.profiler.btrace.dir}/"
					+ "BTraceTaskMemProfile.class,scriptOutputFile=%s");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method will enable dynamic profiling using the BTraceTaskProfile
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "opensimplex.profiler.btrace.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	private static boolean enableProfiling(Configuration conf) {

		// The user must specify the btrace directory
		if (conf.get(PROFILER_BTRACE_DIR) == null) {
			LOG.warn("The parameter 'opensimplex.profiler.btrace.dir' "
					+ "is required to enable profiling");
			return false;
		}

		// set the profiling parameters
		conf.setBoolean(MR_TASK_PROFILE, true);
		conf.set(MR_TASK_PROFILE_MAPS, "0-9999");
		conf.set(MR_TASK_PROFILE_REDS, "0-9999");
		conf.setInt(MR_JOB_REUSE_JVM, 1);
		conf.setInt(MR_NUM_SPILLS_COMBINE, 9999);
		conf.setInt(MR_RED_PARALLEL_COPIES, 1);
		conf.setFloat(MR_RED_IN_BUFF_PERC, 0f);
		conf.setBoolean(MR_MAP_SPECULATIVE_EXEC, false);
		conf.setBoolean(MR_RED_SPECULATIVE_EXEC, false);

		LOG.info("Job profiling enabled");
		return true;
	}

	/**
	 * Gathers the job history files, the task profiles, and data transfers if
	 * requested. Generates the job profile. The generated directory structure
	 * is:
	 * 
	 * outputDir/history/conf.xml <br />
	 * outputDir/history/history_file <br />
	 * outputDir/task_profiles/task.profile <br />
	 * outputDir/job_profiles/job_profile.xml <br />
	 * outputDir/transfers/transfer <br />
	 * 
	 * @param conf
	 *            the MapReduce job configuration
	 * @param outputDir
	 *            the local output directory
	 * @return the job id
	 * @throws IOException
	 */
	public static String gatherJobExecutionFiles(Configuration conf,
			File outputDir) throws IOException {

		// Validate the results directory
		outputDir.mkdirs();
		if (!outputDir.isDirectory()) {
			throw new IOException("Not a valid directory "
					+ outputDir.toString());
		}

		// Gather the history files
		File historyDir = new File(outputDir, "history");
		historyDir.mkdir();
		File[] historyFiles = gatherJobHistoryFiles(conf, historyDir);

		// Load the history information into a job info
		MRJobHistoryLoader historyLoader = new MRJobHistoryLoader(
				historyFiles[0].getAbsolutePath(),
				historyFiles[1].getAbsolutePath());
		MRJobInfo mrJob = historyLoader.getMRJobInfoWithDetails();
		String jobId = mrJob.getExecId();

		if (conf.getBoolean(MR_TASK_PROFILE, false)) {

			// Gather the profile files
			File taskProfDir = new File(outputDir, "task_profiles");
			taskProfDir.mkdir();
			gatherJobProfileFiles(mrJob, taskProfDir);

			// Export the job profile XML file
			File jobProfDir = new File(outputDir, "job_profiles");
			jobProfDir.mkdir();
			File profileXML = new File(jobProfDir, "profile_" + jobId + ".xml");
			exportProfileXMLFile(mrJob, conf, taskProfDir, profileXML);

			// Remove the task profiles if requested
			if (!conf.getBoolean(PROFILER_RETAIN_TASK_PROFS, true)) {
				for (File file : listTaskProfiles(jobId, taskProfDir)) {
					file.delete();
				}
				taskProfDir.delete();
			}
		}

		// Get the data transfers if requested
		if (conf.getBoolean(PROFILER_COLLECT_TRANSFERS, false)) {
			File transfersDir = new File(outputDir, "transfers");
			transfersDir.mkdir();
			gatherJobTransferFiles(mrJob, transfersDir);
		}

		return jobId;
	}

	/**
	 * Copies the two history files (conf and stats) from the Hadoop history
	 * directory of the job (local or on HDFS) to the provided local history
	 * directory.
	 * 
	 * This method returns an array of two files, corresponding to the local
	 * conf and stats files respectively.
	 * 
	 * @param conf
	 *            the Hadoop configuration
	 * @param historyDir
	 *            the local history directory
	 * @return the conf and stats files
	 * @throws IOException
	 */
	public static File[] gatherJobHistoryFiles(Configuration conf,
			File historyDir) throws IOException {

		// Create the local directory
		historyDir.mkdirs();
		if (!historyDir.isDirectory()) {
			throw new IOException("Not a valid results directory "
					+ historyDir.toString());
		}

		// Get the HDFS Hadoop history directory
		Path hdfsHistoryDir = null;
		String outDir = conf.get(HADOOP_HDFS_JOB_HISTORY,
				conf.get(MR_OUTPUT_DIR));
		if (outDir != null && !outDir.equals("none")) {

			hdfsHistoryDir = new Path(new Path(outDir), "_logs"
					+ Path.SEPARATOR + "history");

			// Copy the history files
			File[] localFiles = copyHistoryFiles(conf, hdfsHistoryDir,
					historyDir);
			if (localFiles != null)
				return localFiles;
		}

		// Get the local Hadoop history directory (Hadoop v0.20.2)
		String localHistory = conf
				.get(HADOOP_LOCAL_JOB_HISTORY,
						"file:///"
								+ new File(System.getProperty(HADOOP_LOG_DIR))
										.getAbsolutePath() + File.separator
								+ "history");
		Path localHistoryDir = new Path(localHistory);

		// Copy the history files
		File[] localFiles = copyHistoryFiles(conf, localHistoryDir, historyDir);
		if (localFiles != null)
			return localFiles;

		// Get the local Hadoop history directory (Hadoop v0.20.203)
		String doneLocation = conf.get(HADOOP_COMPLETED_HISTORY);
		if (doneLocation == null)
			doneLocation = new Path(localHistoryDir, "done").toString();

		// Build the history location pattern. Example:
		// history/done/version-1/localhost_1306866807968_/2011/05/31/000000
		String localHistoryPattern = doneLocation
				+ "/version-[0-9]/*_/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/*";

		FileSystem fs = FileSystem.getLocal(conf);
		FileStatus[] status = fs.globStatus(new Path(localHistoryPattern));
		if (status != null && status.length > 0) {
			for (FileStatus stat : status) {
				// Copy the history files
				localFiles = copyHistoryFiles(conf, stat.getPath(), historyDir);
				if (localFiles != null)
					return localFiles;
			}
		}

		// Unable to copy the files
		throw new IOException("Unable to find history files in directories "
				+ localHistoryDir.toString() + " or "
				+ hdfsHistoryDir.toString());
	}

	/**
	 * Builds and returns the job identifier (e.g., job_23412331234_1234)
	 * 
	 * @param conf
	 *            the Hadoop configuration of the job
	 * @return the job identifier
	 */
	public static String getJobId(Configuration conf) {
		String jar = conf.get(MR_JAR);

		Matcher matcher = JOB_PATTERN.matcher(jar);
		if (matcher.find()) {
			return matcher.group(1);
		}

		return "job_";
	}

	/**
	 * Loads system properties common to profiling, job analysis, what-if
	 * analysis, and optimization. The system properties are set in the
	 * bin/config.sh script.
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void loadCommonSystemProperties(Configuration conf) {

		// The BTrace directory for the task profiling
		if (conf.get(Profiler.PROFILER_BTRACE_DIR) == null)
			conf.set(Profiler.PROFILER_BTRACE_DIR,
					System.getProperty(Profiler.PROFILER_BTRACE_DIR));

		// The cluster name
		if (conf.get(Profiler.PROFILER_CLUSTER_NAME) == null)
			conf.set(Profiler.PROFILER_CLUSTER_NAME,
					System.getProperty(Profiler.PROFILER_CLUSTER_NAME));

		// The output directory for the result files
		if (conf.get(Profiler.PROFILER_OUTPUT_DIR) == null)
			conf.set(Profiler.PROFILER_OUTPUT_DIR,
					System.getProperty(Profiler.PROFILER_OUTPUT_DIR));
	}

	/**
	 * Loads system properties related to profiling into the Hadoop
	 * configuration. The system properties are set in the bin/config.sh script.
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void loadProfilingSystemProperties(Configuration conf) {

		// Load the common system properties
		loadCommonSystemProperties(conf);

		// The sampling mode (off, profiles, or tasks)
		if (conf.get(Profiler.PROFILER_SAMPLING_MODE) == null
				&& System.getProperty(Profiler.PROFILER_SAMPLING_MODE) != null)
			conf.set(Profiler.PROFILER_SAMPLING_MODE,
					System.getProperty(Profiler.PROFILER_SAMPLING_MODE));

		// The sampling fraction
		if (conf.get(Profiler.PROFILER_SAMPLING_FRACTION) == null
				&& System.getProperty(Profiler.PROFILER_SAMPLING_FRACTION) != null)
			conf.set(Profiler.PROFILER_SAMPLING_FRACTION,
					System.getProperty(Profiler.PROFILER_SAMPLING_FRACTION));

		// Flag to retain the task profiles
		if (conf.get(Profiler.PROFILER_RETAIN_TASK_PROFS) == null
				&& System.getProperty(Profiler.PROFILER_RETAIN_TASK_PROFS) != null)
			conf.set(Profiler.PROFILER_RETAIN_TASK_PROFS,
					System.getProperty(Profiler.PROFILER_RETAIN_TASK_PROFS));

		// Flag to collect the data transfers
		if (conf.get(Profiler.PROFILER_COLLECT_TRANSFERS) == null
				&& System.getProperty(Profiler.PROFILER_COLLECT_TRANSFERS) != null)
			conf.set(Profiler.PROFILER_COLLECT_TRANSFERS,
					System.getProperty(Profiler.PROFILER_COLLECT_TRANSFERS));
	}
}
