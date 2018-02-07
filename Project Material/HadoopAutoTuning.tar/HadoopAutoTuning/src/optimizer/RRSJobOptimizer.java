
import org.apache.hadoop.conf.Configuration;

import space.ParameterSpace;
import space.ParameterSpacePoint;
import config.ClusterConfiguration;

/**
 * A Job optimizer that uses Recursive Random Search (
 * {@link RecursiveRandomSearch}) in order to find the best configuration
 * parameter settings.
 * 
 * @author VinayKhedekar
 */
public class RRSJobOptimizer extends JobOptimizer implements
		IRRSCostEngine<ParameterSpacePoint> {

	/**
	 * Constructor
	 * 
	 * @param jobOracle
	 *            the job profile oracle
	 * @param dataModel
	 *            the data set model
	 * @param scheduler
	 *            the scheduler
	 * @param cluster
	 *            the cluster setup
	 * @param conf
	 *            the current configuration settings
	 */
	public RRSJobOptimizer(JobProfileOracle jobOracle, DataSetModel dataModel,
			IWhatIfScheduler scheduler, ClusterConfiguration cluster,
			Configuration conf) {
		super(jobOracle, dataModel, scheduler, cluster, conf);
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */
	@Override
	protected ParameterSpacePoint optimizeInternal() {

		// Initialize the space
		ParameterSpace space = ParamSpaceUtils.getFullParamSpace(currConf);

		// Perform recursive random search to find the best point
		RecursiveRandomSearch<ParameterSpacePoint> rrs = 
			new RecursiveRandomSearch<ParameterSpacePoint>(currConf);
		return rrs.findBestSpacePoint(space, this);
	}

	/**
	 * @see IRRSCostEngine#costSpacePoint(Object)
	 */
	@Override
	public double costSpacePoint(ParameterSpacePoint point) {
		point.populateConfiguration(currConf);
		return whatif(currConf);
	}

}
