# Distributed Subtrajectory Similarity Matrix (DSimM)

An open source implementation of the Distributed Subtrajectory Similarity Matrix solution by employing subtrajectory join procedure that was proposed in [[1](https://dl.acm.org/doi/10.1145/3373642)] and the similarity function that was proposed in proposed in [[2](https://doi.org/10.1109/BigData47090.2019.9005563)].

## Implementation Details
This is a MapReduce solution in Java that has been implemented and tested against Hadoop 2.7.2. The only external library used here is [cts](https://github.com/orbisgis/cts) for coordinate transformation.

## Input Data
The input is an hdfs directory containing csv files (comma delimited) of the form <obj_id, traj_id, t, lon, lat>, where 
* obj_id and traj_id are integers (traj_id might be ommited if not available)
* t is an integer corresponding to the unix timestamp
* lon and lat are the coordinates in WGS84

## Preprocessing
For each dataset that is going to be used a [preprocessing step](https://github.com/DataStories-UniPi/Distributed-Subtrajectory-Similarity-Matrix/blob/master/src/DSimM/PreprocessDriver.java) must take place before the first run of DSimM.
### Input Parameters
The input parameters of this preprocessing step are the following:
* hostname --> is a string representing the hostname (e.g. localhost)
* dfs_port --> is a string representing the hdfs port
* rm_port -->  is a string representing the YARN resource manager port
* workspace_dir --> is a string corresponding to the HDFS path of your workspace;
* raw_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the raw input data are stored
* prep_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the intermediate preprocessed output data will be stored
* input_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the final preprocessed output data will be stored
* sample_freq --> is a double precision number corresponding to the probability with which a key will be chosen, during the sampling phase of the repartitioning and index building (refer to [[1](https://doi.org/10.1109/BigData47090.2019.9005563)] for more details)
* max_nof_samples =--> is an integer corresponding to the total number of samples to obtain from all selected splits, during the sampling phase of the repartitioning and index building (refer to [[1](https://doi.org/10.1109/BigData47090.2019.9005563)] for more details)
* maxCellPtsPrcnt -->  is a double precision number corresponding to the maximum percent of points (in relation to the total number of points) per cell of the quadtree index (refer to [[1](https://doi.org/10.1109/BigData47090.2019.9005563)] for more details)

Example --> yarn jar Preprocess.jar "hdfs://localhost" ":9000" ":8050" "/workspace_dir" "/raw" "/prep" "/input" sample_freq max_nof_samples maxCellPtsPrcnt

## DSimM
[DSimM](https://github.com/DataStories-UniPi/Distributed-Subtrajectory-Similarity-Matrix/blob/master/src/DSimM/DSimMDriver.java) can be run multiple times with different parameters once the preprocessing step has taken place.

### Input Parameters
The input parameters of DSimM are the following:
* hostname --> is a string representing the hostname (e.g. localhost)
* dfs_port --> is a string representing the hdfs port
* rm_port -->  is a string representing the YARN resource manager port
* workspace_dir --> is a string corresponding to the HDFS path of your workspace;
* input_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the input of DSimM is stored
* intermdt_output_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the intermediate output of DSimM will be stored
* subtraj_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the files containing information about the discovered subtrajectories will be stored (refer to [[1](https://doi.org/10.1109/BigData47090.2019.9005563)] for more details)
* output_dir --> is a string corresponding to the relative HDFS path (in relation with the workspace_dir) where the output of DSc will be stored
* nof_reducers --> is an integer corresponding to the number of reducers
* e_sp_method --> can be either 1 or 2 corresponding to the two alternative distance range methods available. 1 is for pure euclidean distance and 2 is for percentage of the quadtree cell that the point belongs to (refer to [[1](https://doi.org/10.1109/BigData47090.2019.9005563)] for more details)
* epsilon_sp --> is an integer number corresponding to the euclidean distance if e_sp_method = 1 and a double precision number corresponding to the percentage of the quadtree cell if e_sp_method = 2
* epsilon_t  --> is an integer number corresponding to ε<sub>t</sub> in seconds
* dt --> is an integer number corresponding to δt in seconds
* SegmentationAlgorithm --> can be either 1 or 2 corresponding to the two alternative segmentation algorithms
* w --> is an integer number corresponding to the sliding window size of the segmentation algorithm as defined in [[1](https://doi.org/10.1109/BigData47090.2019.9005563)]
* tau --> is a double precision number corresponding to the τ parameter of the segmentation algorithm as defined in [[1](https://doi.org/10.1109/BigData47090.2019.9005563)]
* job_description = is a string corresponding to the name of the current run (this is helpfull when multiple runs with different parameters take place)

Example --> yarn jar DSimM.jar "hdfs://localhost" ":9000" ":8050" /workspace_dir" "/input" "/intermdt_output" "/subtraj" "/clust_output" nof_reducers e_sp_method epsilon_sp epsilon_t dt SegmentationAlgorithm w tau DSimM

## Acknowledgement
This work was partially supported by the European Union’s Horizon 2020 research and innovation programme under grant agreements No 780754 (Track & Know) and by the Greek Ministry of Development andInvestment, General Secretariat of Research and Technology, under the Operational Programme Competitiveness, Entrepreneurship and Innovation 2014-2020 (grant T1EDK-03268, i4sea).

## References
1. P. Tampakis, C. Doulkeridis, N. Pelekis, and Y. Theodoridis. “Distributed Subtrajectory Join on Massive Datasets”. In:ACM Trans. Spatial AlgorithmsSyst.6.2 (2019)
2. P. Tampakis, N. Pelekis, C. Doulkeridis, and Y. Theodoridis. “Scalable Distributed Subtrajectory Clustering”. In:IEEE BigData 2019. 2019, pp. 950–959
