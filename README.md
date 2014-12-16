# VarCrunch

VarCrunch is a germline and somatic variant caller that runs on Hadoop using the Apache (S)Crunch API

The variant calling algorithms themselves are from Guacamole, but VarCrunch serves as a wrapper to process DNA sequencing reads on Hadoop using MapReduce.

## Usage

### Building

```sh
mvn -DskipTests=true
```


### Running
```sh
yarn jar yarn-*-job.jar
```

```sh
Valid program names are:
  germline: Standard germline variant caller
  readdepth-hist: Computes distribution of read depths
  readdepth-intervals: Computes read depth over a given size interval
  somatic: Standard somatic variant caller, takes tumor/normal input
```