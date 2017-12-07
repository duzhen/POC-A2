from mrjob.job import MRJob, MRStep
import statistics

class AirborneRadioActivity(MRJob):
    # MRJob.PARTITIONER = 'org.apache.hadoop.mapred.lib.HashPartitioner'
    # MRJob.PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'
    # MRJob.PARTITIONER = 'org.apache.hadoop.mapreduce.lib.partition.BinaryPartitioner'
    MRJob.PARTITIONER = 'org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner'

    MRJob.HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    MRJob.HADOOP_OUTPUT_FORMAT = 'org.apache.hadoop.mapred.TextOutputFormat'

    MRJob.local_tmp_dir = './'

    # MRJob.HADOOP_OUTPUT_FORMAT = 'org.apache.hadoop.mapred.FileOutputFormat'
    # MRJob.OUTPUT_PROTOCOL = ReprProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   # combiner=self.combiner_count_words,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        datarow = line.split(',')
        location = datarow[0]
        date = datarow[2]
        data = datarow[7]
        year = date.split('-')[0]
        yield 'standard deviation of '\
              +location+' on '+year, float(data)
        yield 'median of '+location \
              + ' on ' + year, float(data)
        yield 'max of ' + location \
              + ' on ' + year, float(data)
        yield 'min of ' + location \
              + ' on ' + year, float(data)

    def reducer(self, key, values):
        if 'standard' in key:
            yield key, statistics.stdev(values)
        elif 'median of' in key:
            yield key, statistics.median(values)
        elif 'max of' in key:
            yield key, max(values)
        elif 'min of' in key:
            yield key, min(values)

        # print(MRJob.partitioner(self))

if __name__ == '__main__':
    AirborneRadioActivity.run()