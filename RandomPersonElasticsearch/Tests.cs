using Elasticsearch.Net;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace RandomPersonElasticsearch
{
    class ESTests
    {
        public static void InsertIntoES(string csv_filename, ElasticLowLevelClient lowLevelClient, bool isAsync, int bulkLimit, int shardCount, int replicaCount) {

            var idelResponse = lowLevelClient.IndicesDelete<BytesResponse>("people");

            var icrResponse = lowLevelClient.IndicesCreate<BytesResponse>("people", PostData.Serializable(
                new
                {
                    settings = new
                    {
                        number_of_shards = shardCount,
                        number_of_replicas = replicaCount
                    },
                    mappings = new
                    {
                        _doc = new
                        {
                            properties = new
                            {
                                FirstName = new { type = "text" },
                                LastName = new { type = "text" },
                                UserName = new { type = "text" },
                                SSC_Grade = new { type = "text" },
                                HSC_Grade = new { type = "text" },
                                Bachelor_Grade = new { type = "text" },
                                Age = new { type = "integer" },
                                Gender = new { type = "text" },
                                Email = new { type = "text" },
                                DateOfBirth = new { type = "date" },
                                Streent = new { type = "text" },
                                Suite = new { type = "text" },
                                City = new { type = "text" },
                                ZipCode = new { type = "text" },
                            }
                        }
                    }
                }
            ));

            var people = new List<object>
            {
                new { index = new { _index = "people", _type = "_doc" }}
            };


            int counter = 0;
            int id = 0;


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

                    if (id == 0)
                    {
                        people = new List<object>
                        {

                        };
                    }

                    if (counter >= bulkLimit)
                    {
                        if(isAsync)
                        {
                            lowLevelClient.BulkAsync<StringResponse>(PostData.MultiJson(people));
                        }else
                        {
                            lowLevelClient.Bulk<StringResponse>(PostData.MultiJson(people));
                        }

                        counter = 0;
                        people = new List<object>
                        {

                        };
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
                        id++;
                    }


                    Console.WriteLine(id);
                }


                if (isAsync)
                {
                    lowLevelClient.BulkAsync<StringResponse>(PostData.MultiJson(people));
                }
                else
                {
                    lowLevelClient.Bulk<StringResponse>(PostData.MultiJson(people));
                }                
            }
        }
    }
}
