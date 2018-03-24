using System;
using System.Collections.Generic;
using System.IO;
using Elasticsearch.Net;
using System.Diagnostics;

namespace RandomPersonElasticsearch

{
    public class Person
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }
        public string SSC_Grade { get; set; }
        public string HSC_Grade { get; set; }
        public string Bachelor_Grade { get; set; }
        public int Age { get; set; }
        public string Gender { get; set; }
        public string Email { get; set; }
        public DateTime DateOfBirth { get; set; }
        public string Street { get; set; }
        public string Suite { get; set; }
        public string City { get; set; }
        public string ZipCode { get; set; }
    }


    class Program
    {

        static string csv_filename = "RandomPersons1L.csv";
        static string test_results = "test1.csv";
        static string test_columns = "name, rowCount, bulklimit, shard, replica, elapsed_time";


        static void Main(string[] args)
        {

            var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"))
                .RequestTimeout(TimeSpan.FromMinutes(5));


            var lowLevelClient = new ElasticLowLevelClient(settings);

            List<string> results = new List<string>();

            //ESTests.InsertIntoES()
            //ESTests.InsertIntoES(csv_filename, lowLevelClient, 30000, 3, 2);

            // 10, 10 -> 3:26
            // 3, 5 -> 2:48
            // 3, 2 -> 2:50


            for (int i = 30000; i <= 50000; i += 5000)
            {
                for (int shardCount = 1; shardCount <= 10; shardCount += 2)
                {
                    for (int replicaCount = 1; replicaCount <= 10; replicaCount += 2)
                    {

                        Stopwatch sw = new Stopwatch();

                        sw.Start();

                        ESTests.InsertIntoES(csv_filename, lowLevelClient, false, i, 3, 2);

                        sw.Stop();

                        results.Add($"{"RandomPersons1L"}, 100000, {i}, {shardCount}, {replicaCount}, {sw.Elapsed}");
                    }
                }


            }

            using (StreamWriter swriter = new StreamWriter(test_results))
            {
                swriter.WriteLine(test_columns);
                foreach (var line in results)
                {
                    swriter.WriteLine(line);
                }

            }



            Console.ReadLine();
        }
    }
}
