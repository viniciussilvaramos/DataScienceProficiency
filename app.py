# Datasets:
# 	July -> ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
# 	August -> ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz

from pyspark import SparkConf, SparkContext

sc = SparkContext()


july = sc.textFile('access_log_Jul95').cache()
august = sc.textFile('access_log_Aug95').cache()

class UniqueHosts(object):

    def show(self):
        unique_host_july = july.flatMap(lambda line: line.split(' ')[0]).distinct().count()
        unique_host_august = august.flatMap(lambda line: line.split(' ')[0]).distinct().count()

        print("Unique Hosts on July: {}".format(unique_host_july))
        print("Unique Hosts on August: {}".format(unique_host_august))

class Errors404(object):

    def __init__(self):
        self.july_404 = july.filter(self.check_if_404).cache()
        self.august_404 = august.filter(self.check_if_404).cache()

    def check_if_404(self, line):
        try:
            code = line.split(' ')[-2]
            if code == '404':
                return True
        except:
            pass
        return False


    def get_daily_count(self, rdd, month):
        days = rdd.map(lambda line: line.split('[')[1].split(':')[0])
        counts = days.map(lambda day: (day, 1)).reduceByKey(lambda x,y: x + y).collect()
        
        print('{}: 404 errors per day:'.format(month))
        for day, count in counts:
            print(day, count)
            
        return counts
    
    def get_top5_count(self, rdd, month):
        endpoints = rdd.map(lambda line: line.split('"')[1].split(' ')[1])
        counts = endpoints.map(lambda endpoint: (endpoint, 1)).reduceByKey(lambda x,y: x + y)
        top = counts.sortBy(lambda pair: -pair[1]).take(5)
        
        print('{}: Top 5 most frequent 404 endpoints:'.format(month))
        for endpoint, count in top:
            print(endpoint, count)
            
        return top

    def show_count(self):
        print('404 errors in July: {}'.format(self.july_404.count()))
        print('404 errors in August {}'.format(self.august_404.count()))
        return self
    
    def show_daily_count(self):
        self.get_daily_count(self.july_404, "July")
        self.get_daily_count(self.august_404, "August")
        return self
    
    def show_top5_count(self):
        self.get_top5_count(self.july_404, "July")
        self.get_top5_count(self.august_404, "August")
        return self

class ByteCount(object):

    def byte_count(self, line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                raise ValueError()
            return count
        except:
            return 0

    def show(self):
        july_count = july.map(self.byte_count).reduce(lambda x,y: x + y)
        august_count = august.map(self.byte_count).reduce(lambda x,y: x + y)

        print('Total bytes in July: {}'.format(july_count))
        print('Total bytes in July: {}'.format(august_count))

if __name__ == '__main__':
    UniqueHosts() \
        .show()

    Errors404() \
        .show_count() \
        .show_daily_count() \
        .show_top5_count()
    
    ByteCount() \
        .show()

    sc.stop()

