using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using DgraphNet.Client.Proto;
using Google.Protobuf;
using Grpc.Core;
using JSON;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static DgraphNet.Client.DgraphNetClient;

namespace DgraphNet.Client.Sample
{


    class Program
    {
        static void Main(string[] args)
        {

            var connection = new DgraphConnection("localhost", 9080, ChannelCredentials.Insecure);

            var pool = new DgraphConnectionPool().Add(connection);

            var client = new DgraphNetClient(pool);

            client.Alter(new Operation { DropAll = true });

            //Index le schéma 
            string schema = "domain_name_index: string @index(hash) .";
            Operation op = new Operation { Schema = schema };
            client.Alter(op);



            using (Transaction txn = client.NewTransaction())
            {
                /***************** JSON MIL TEST*********************/
                //Ouvre le json cible
                JObject o1 = JObject.Parse(File.ReadAllText(@"c:\marche.json"));
                Dictionary<string, JToken> dic_proprety = new Dictionary<string, JToken>();


                foreach (JProperty property in o1.Properties())
                {
                    if (property.Value.HasValues)
                    {
                        dic_proprety.Add(property.Name, property.Value);
                    }
                }

                foreach (var item in dic_proprety)
                {
                    if (item.Value.GetType() == typeof(JArray))
                    {
                        if (!item.Value.HasValues)
                        {
                            dic_proprety.Remove(item.Key);
                        }
                        var lolae = item.Value;
                        JArray qs = (JArray)lolae;
                        for (int i = 0; i < qs.Count; i++)
                        {
                            if (qs[i].GetType() == typeof(JArray))
                            {
                                qs.RemoveAt(i);
                            }
                        }
                    }
                }

                JObject k = new JObject();
                int b = 0;


                foreach (var item in dic_proprety)
                {
                    if (item.Value.HasValues)
                    {
                        JArray array = new JArray();
                        b++;
                        JObject obj = new JObject(
                           new JObject(
                               new JProperty("name_value_list" + b, item.Value)
                               )
                       );
                        array.Add(obj);
                        JObject objSort = new JObject();
                        objSort["list" + b] = array;
                        JObject u = new JObject();
                        u.Add("obj" + b, objSort);
                        k.Merge(u, new JsonMergeSettings { MergeArrayHandling = MergeArrayHandling.Union });
                    }

                }
                k.Add("domain_name_index", "Millenium");


                //Parse et seria le json cible
                var json2 = JsonConvert.SerializeObject(k);
                string json3 = json2.Replace("true", "0");
                string json4 = json3.Replace("false", "1");
                Console.WriteLine(k);


                // Run mutation

                //JObject o7 = JObject.Parse(File.ReadAllText(@"c:\Millenium_sorted.json"));
                //var json77 = JsonConvert.SerializeObject(o7);



                Mutation mu = new Mutation { SetJson = ByteString.CopyFromUtf8(json4) };
                txn.Mutate(mu);
                txn.Commit();


                Console.ReadLine();
                //================CREATE DATA=====================
                /* Person p = new Person();
                 p.Name = "Marceau";

                */


            }
            // Query
            /* string query =
               "query all($a: string){\n" +
               "  all(func: eq(name,$a)) {\n" +
               "    name\n" +
               "  }\n" +
               "}\n";

               IDictionary<string, string> vars = new Dictionary<string, string>
               {
                   { "$a", "Marceau" }
               };

               Response res = client.NewTransaction().QueryWithVars(query, vars);

               //Deserialize
               People ppl = JsonConvert.DeserializeObject<People>(res.Json.ToStringUtf8());

               //Print results
               Console.WriteLine($"people found: {ppl.All.Count}");

               foreach (var p in ppl.All)
               {
                   Console.WriteLine(p.Name);

               }
               Console.ReadLine();*/

        }

        /*class Person
        {
            public string Name { get; set; }
            public Person() { }

        }


        class People
        {
            public List<Person> All { get; set; }
            public People() { }
        }*/



    }





}
