using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace YRedis.Test
{
    class Program
    {

        public static void Main(string[] args)
        {
            var _contentRootPath = Directory.GetCurrentDirectory();
            RedisHelper.InitRedis("10.99.20.63:7001,name=YRedis,connectTimeout=180,password=abcd-1234");

            HashSetTest();
            HashGetTest();
            HashGetAllTest();
            Debug.WriteLine(RedisHelper.HLen("TestHashID"));

            Debug.WriteLine(RedisHelper.HDel("TestHashID", "1"));
            Debug.WriteLine(RedisHelper.HDel("TestHashID", new string[] { "1", "2", "3", "4" }));
            Debug.WriteLine(RedisHelper.HExists("TestHashID", "1"));
            Debug.WriteLine(RedisHelper.HExists("TestHashID", "99"));

            var keys = RedisHelper.HKeys("TestHashID");
            foreach (var item in keys)
            {
                Debug.WriteLine(item);
            }

            var values = RedisHelper.HValues<string>("TestHashID");
            foreach (var item in values)
            {
                Debug.WriteLine(item);
            }


            Console.ReadKey();


            Task.Factory.StartNew(() =>
            {
                int index = 0;
                while (true)
                {
                    var sub = RedisHelper.Redis.GetSubscriber();
                    sub.Publish("message", "value" + index++);
                }
            });
            Task.Factory.StartNew(() =>
            {
                var sub = RedisHelper.GetSubscriber();
                sub.Subscribe("message", (channel, message) =>
                {
                    Console.WriteLine(message);
                    // Thread.Sleep(1000);
                });
            });
            Task.Factory.StartNew(() =>
            {
                var sub = RedisHelper.Redis.GetSubscriber();
                sub.Subscribe("message", (channel, message) =>
                {
                    Console.WriteLine("Task111111111111111111111111111111111111111:" + message);
                    //Thread.Sleep(1000);
                });
            });
            //Task.Factory.StartNew(() =>
            //{
            //    var index = 0;
            //    while (true)
            //    {
            //        RedisHelper.Set("key" + index, "value" + index);
            //        index++;
            //    }
            //});
            //Task.Factory.StartNew(() =>
            //{
            //    var index = 0;
            //    while (true)
            //    {
            //        RedisHelper.HSet("HashId1", Guid.NewGuid().ToString(), "value" + index);
            //        index++;
            //    }
            //});


            Console.ReadKey();
        }

        public static void HashSetTest()
        {
            var res = RedisHelper.KeyDelete("TestHashID");
            for (int i = 0; i < 100; i++)
            {
                if (i == 78)
                    RedisHelper.HSet("TestHashID", "78", "");
                else
                    RedisHelper.HSet("TestHashID", i.ToString(), i.ToString());
            }
        }

        public static void HashGetTest()
        {
            var res = RedisHelper.HGet<string>("TestHashID", "0");
            Debug.WriteLine("HashGet:" + res);
            Debug.WriteLine("===================================");
            var resList = RedisHelper.HGet<string>("TestHashID", new string[] { "1", "200", "6", "78", "133", "300", "100", "56", "99" });
            foreach (var item in resList)
            {
                Debug.WriteLine(item);
            }
        }

        public static void HashGetAllTest()
        {
            var resList = RedisHelper.HGetAll<string>("TestHashID");
            foreach (var item in resList)
            {
                Debug.WriteLine(item);
            }
        }
    }
}
