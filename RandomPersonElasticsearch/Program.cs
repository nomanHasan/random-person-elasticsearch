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

        static string csv_filename = "RandomPersons10000.csv";

        static void Main(string[] args)
        {

            var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"))
                .RequestTimeout(TimeSpan.FromMinutes(5));


            var lowLevelClient = new ElasticLowLevelClient(settings);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            
            int bulkLimit = 1000;
            int counter = 0;
            int id = 0;


            var people = new List<object>
            {
                new { index = new { _index = "people", _type = "_doc" }}
            };

            var person = new Person
            {
                FirstName = "Kuddus",
                LastName = "Miah"
            };

            var indexCreateResponse = lowLevelClient.Index<BytesResponse>("people", "_doc", PostData.Serializable(person));


            using (StreamReader reader = new StreamReader(csv_filename))
            {
                reader.ReadLine();
                while (true)
                {
                    string line = reader.ReadLine();
                    if (line == null)
                    {
                        break;
                    }
                    
                    var values = line.Split(",");
                    
                    if (counter >= bulkLimit)
                    {
                        var indexResponse = lowLevelClient.Bulk<StringResponse>(PostData.MultiJson(people));
                        counter = 0;
                        people = new List<object>
                        {
                           
                        };
                        Console.WriteLine("Bulk Insert");
                    }
                    else
                    {
                        people.Add(new { index = new { _index = "people", _type = "_doc", _id = $"{id}" } });
                      
                        people.Add(
                        new
                        {
                            FirstName = values[0],
                            LastName = values[1],
                            UserName = values[2],
                            SSC_Grade = values[3],
                            HSC_Grade = values[4],
                            Bachelor_Grade = values[5],
                            Age = Int32.Parse(values[6]),
                            Gender = values[7],
                            Email = values[8],
                            DateOfBirth = DateTime.Parse(values[9]),
                            Street = values[10],
                            Suite = values[11],
                            City = values[12],
                            ZipCode = values[13],
                        });
                        counter++;
                    }

                    id++;

                    //Console.WriteLine(line);
                }
            }

            sw.Stop();

            Console.WriteLine("Time Elapsed: " + sw.Elapsed);

            Console.ReadLine();
        }
    }
}
