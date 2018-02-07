package edu.duke.starfish.whatif;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.whatif.data.MapInputSpecs;

/**
 * Utility functions in the What-if package
 * 
 * @author hero
 */
public class WhatIfUtils {

	/**
	 * Creates and returns a list of map input specifications based on the
	 * source job
	 * 
	 * @param job
	 *            the MapReduce job
	 * @return list of map input specifications
	 */
	public static List<MapInputSpecs> generateMapInputSpecs(MRJobInfo job) {

		// Get the map attempts
		List<MRMapAttemptInfo> attempts = job
				.getMapAttempts(MRExecutionStatus.SUCCESS);
		List<MapInputSpecs> specs = new ArrayList<MapInputSpecs>(
				attempts.size());
		if (attempts.size() == 0)
			return specs;

		// Create the map input specifications
		for (MRMapAttemptInfo attempt : attempts) {

			MRMapProfile mapProf = (MRMapProfile) attempt.getProfile();
			int inputIndex = mapProf.getInputIndex();
			long size = mapProf.getCounter(MRCounter.HDFS_BYTES_READ,
					mapProf.getCounter(MRCounter.S3N_BYTES_READ, 0l));
			boolean isCompressed = mapProf.getStatistic(
					MRStatistics.INPUT_COMPRESS_RATIO, 1d) != 1d;
			MapInputSpecs spec = new MapInputSpecs(inputIndex, 1, size,
					isCompressed, DataLocality.DATA_LOCAL);
			specs.add(spec);
		}

		// Summarize the input specifications
		List<MapInputSpecs> result = new ArrayList<MapInputSpecs>();

		// Sort the specs based on input index and then length
		Collections.sort(specs, new Comparator<MapInputSpecs>() {
			@Override
			public int compare(MapInputSpecs arg0, MapInputSpecs arg1) {
				if (arg0.getInputIndex() == arg1.getInputIndex()) {
					if (arg0.getSize() == arg1.getSize())
						return 0;
					else
						return arg0.getSize() < arg1.getSize() ? 1 : -1;
				} else {
					return arg0.getInputIndex() > arg1.getInputIndex() ? 1 : -1;
				}
			}
		});

		// Group specs with same input index and similar lengths together
		int groupCount = 1;
		int groupIndex = specs.get(0).getInputIndex();
		double groupSum = specs.get(0).getSize();
		double groupInitLength = specs.get(0).getSize();
		boolean groupCompr = specs.get(0).isCompressed();

		int numspecs = specs.size();
		for (int j = 0; j < numspecs - 1; ++j) {

			if (groupIndex == specs.get(j + 1).getInputIndex()
					&& (groupInitLength - specs.get(j + 1).getSize())
							/ groupInitLength < 0.2) {
				// Split length within 20% of the first one in group
				++groupCount;
				groupSum += specs.get(j + 1).getSize();

			} else {
				// Create an input spec for this group
				result.add(new MapInputSpecs(groupIndex, groupCount,
						(long) (groupSum / groupCount), groupCompr,
						DataLocality.DATA_LOCAL));

				groupCount = 1;
				groupIndex = specs.get(j + 1).getInputIndex();
				groupSum = specs.get(j + 1).getSize();
				groupInitLength = specs.get(j + 1).getSize();
				groupCompr = specs.get(j + 1).isCompressed();
			}
		}

		// Add the input specs for the last set of specs
		result.add(new MapInputSpecs(groupIndex, groupCount,
				(long) (groupSum / groupCount), groupCompr,
				DataLocality.DATA_LOCAL));

		return result;
	}

	/**
	 * Get the memory required by a map task in terms of bytes
	 * 
	 * @param mapProfile
	 *            the (virtual) map profile
	 * @return the memory requirements in bytes
	 */
	public static long getMapMemoryRequired(MRMapProfile mapProfile) {

		if (mapProfile == null)
			return 0l;

		double memory = mapProfile.getStatistic(MRStatistics.STARTUP_MEM, 0d)
				+ mapProfile.getStatistic(MRStatistics.SETUP_MEM, 0d)
				+ mapProfile.getStatistic(MRStatistics.CLEANUP_MEM, 0d);

		memory += mapProfile.getStatistic(MRStatistics.MAP_MEM_PER_RECORD, 0d)
				* mapProfile.getCounter(MRCounter.MAP_INPUT_RECORDS, 0l);

		return Math.round(memory);
	}

	/**
	 * Get the memory required by a reduce task in terms of bytes
	 * 
	 * @param redProfile
	 *            the (virtual) reduce profile
	 * @return the memory requirements in bytes
	 */
	public static long getReduceMemoryRequired(MRReduceProfile redProfile) {

		if (redProfile == null || redProfile.isEmpty())
			return 0l;

		double memory = redProfile.getStatistic(MRStatistics.STARTUP_MEM, 0d)
				+ redProfile.getStatistic(MRStatistics.SETUP_MEM, 0d)
				+ redProfile.getStatistic(MRStatistics.CLEANUP_MEM, 0d);

		memory += redProfile.getStatistic(MRStatistics.REDUCE_MEM_PER_RECORD,
				0d) * redProfile.getCounter(MRCounter.REDUCE_INPUT_RECORDS, 0l);

		return Math.round(memory);
	}

}
