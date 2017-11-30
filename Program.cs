using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using OSISoftCompletionData;
using System.Data.SqlClient;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace SampleSender
{

    public class OSIItem
    {
        [JsonProperty(ItemConverterType = typeof(JavaScriptDateTimeConverter))]
        public DateTime Timestamp { get; set; }
        public string Value { get; set; }
        public string UnitsAbbreviation { get; set; }
        public bool Good { get; set; }
        public bool Questionable { get; set; }
        public bool Substuituted { get; set; }
    }

    public class Program
    {
        static string sqlConnectionString = "Server=tcp:dvnhckthon-clr.database.windows.net,1433;Initial Catalog=completions;Persist Security Info=False;User ID=hacker;Password=asdfadsafdsf;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

        public static void Main(string[] args)
        {
            loadPIData();
        }

        private static void loadPIData()
        {

            var username = "hacker";
            var password = "Pa$$w0rd";
            var client = new PIWebAPIClient(username, password);

            string pressure = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQfy8liDPVAlkrZEtw0Fh1wgT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xUUkVBVElORyBQUkVTU1VSRQ/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";
            string slurrytotal = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQa89DYKgMTlMuXLgXHniPvAT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xTTFVSUlkgVE9UQUw/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";
            string slurryrate = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQcEDba2FtgFkj3SMDEZwBswT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xTTFVSUlkgUkFURQ/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";
            string blenderpropconc = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQHN-mPjB1alEYAvezJAEdoQT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xCTEVOREVSIFBST1AgQ09OQw/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";
            string blenderproptotal = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQkvD3Bcu-DlUONGgrz-y0cAT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xCTEVOREVSIFBST1AgVE9UQUw/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";
            string stage = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQ5uD6lW622l41riCZoX1AuAT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xTVEFHRSBBVCBCTEVOREVS/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";
            string wellname = "https://pi.dvnhackathon.com/piwebapi/streams/A0EkmIZsN6_90OPP6Ztf91JxQQG-r8JTT5xGpRgANOmHTrQiUnrl8rj_l0vh2Ywz6NfjQT1NJU09GVFBJLTAwMVxERVZPTlxTS0lEU1xTS0lEICM3N3xXRUxMTkFNRQ/interpolated?endtime=11-2-2017&starttime=11-1-2017&interval=00:01:00";

            var pressureData = client.GetRequest(pressure);
            var pressureItems = pressureData["Items"].ToString();
            WriteDataToDatabase(pressureItems, "TreatingPressure");

            var slurrytotalData = client.GetRequest(slurrytotal);
            var slurrytotalItems = slurrytotalData["Items"].ToString();
            WriteDataToDatabase(slurrytotalItems, "SlurryTotal");

            var slurryrateData = client.GetRequest(slurryrate);
            var slurryrateItems = slurryrateData["Items"].ToString();
            WriteDataToDatabase(slurryrateItems, "SlurryRate");

            var blenderpropconcData = client.GetRequest(blenderpropconc);
            var blenderpropconcItems = blenderpropconcData["Items"].ToString();
            WriteDataToDatabase(blenderpropconcItems, "BlenderPropConc");

            var blenderproptotalData = client.GetRequest(blenderproptotal);
            var blenderproptotaltems = blenderproptotalData["Items"].ToString();
            WriteDataToDatabase(blenderproptotaltems, "BlenderPropConc");

            var stageData = client.GetRequest(stage);
            var stageltems = stageData["Items"].ToString();
            WriteDataToDatabase(stageltems, "Stage");

            var wellNameData = client.GetRequest(wellname);
            var wellnameItems = wellNameData["Items"].ToString();
            WriteDataToDatabase(wellnameItems, "Wellname");
        }

        private static void WriteDataToDatabase(string json, string tablename)
        {
            using (SqlConnection cnn = new SqlConnection(sqlConnectionString))
            {
                var dt = JsonConvert.DeserializeObject<List<OSIItem>>(json);

                var sql = string.Format("insert into {0} (Timestamp, Value, UnitsAbbreviation,Good,Questionable) values(@Timestamp,@Value,@UnitsAbbreviation, @Good, @Questionable )", tablename);
                cnn.Open();
                foreach (var d in dt)
                {
                    using (SqlCommand cmd = new SqlCommand(sql, cnn))
                    {
                        cmd.Parameters.AddWithValue("@Timestamp", d.Timestamp);
                        cmd.Parameters.AddWithValue("@Value", d.Value);
                        cmd.Parameters.AddWithValue("@UnitsAbbreviation", d.UnitsAbbreviation);
                        cmd.Parameters.AddWithValue("@Good", d.Good);
                        cmd.Parameters.AddWithValue("@Questionable", d.Questionable);

                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }










        //static string connectionString = "Endpoint=sb://pistream.servicebus.windows.net/;SharedAccessKeyName=default;SharedAccessKey=q9LDw+r0g0FxIs/2+hi2lmrF98aLTqpj/SgUGqGYYpo=;EntityPath=pistreameventhub";
        //static string eventHubName = "pistreameventhub";

        //public static void Main(string[] args)
        //{
        //    Console.WriteLine("Press Enter to start now");
        //    Console.ReadLine();
        //    SendingRandomMessages();
        //}

        //static void SendingRandomMessages()
        //{
        //    var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString);
        //    while (true)
        //    {
        //        try
        //        {
        //            var message = Guid.NewGuid().ToString();
        //            Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, message);
        //            var t = eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
        //            t.Wait();
        //        }
        //        catch (Exception exception)
        //        {
        //            Console.ForegroundColor = ConsoleColor.Red;
        //            Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
        //            Console.ResetColor();
        //        }

        //        Thread.Sleep(200);
        //    }
        //}
    }
}