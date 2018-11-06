using System;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using StackExchange.Redis;
using Newtonsoft.Json.Converters;

namespace YRedis
{
    /// <summary>
    /// RedisHelper
    /// </summary>
    public static class RedisHelper
    {
        #region StackExchangeRedis

        private const string _defRedisConStr = "localhost:6379,name=YRedis,connectTimeout=180,password=";
        private static string _redisConStr;
        private static ConnectionMultiplexer _redis;
        private static readonly object _locker = new object();
        private static bool _initialized = false;

        private static IsoDateTimeConverter _timeFormat;
        /// <summary>
        /// Json序列化时间格式
        /// </summary>
        public static IsoDateTimeConverter TimeFormat
        {
            get
            {
                if (_timeFormat == null)
                {
                    _timeFormat = new IsoDateTimeConverter()
                    {
                        DateTimeFormat = "yyyy-MM-dd HH:mm:ss"
                    };
                }
                return _timeFormat;
            }
            set
            {
                _timeFormat = value;
            }
        }

        /// <summary>
        /// Redis初始化完毕
        /// </summary>
        public static bool Initialized
        {
            get
            {
                return _initialized;
            }
        }

        /// <summary>
        /// RedisConnectionString
        /// </summary>
        private static string RedisConStr
        {
            get
            {
                if (string.IsNullOrEmpty(_redisConStr))
                    return _defRedisConStr;
                else
                    return _redisConStr;
            }
        }

        /// <summary>
        /// 初始化Redis
        /// default:"localhost:6379,name=RedisHelper,connectTimeout=180,password="
        /// </summary>
        /// <param name="conStr">Redis连接串</param>
        public static void InitRedis(string conStr)
        {
            _redisConStr = conStr;
            UnInitRedis();
            lock (_locker)
            {
                if (_redis == null)
                {
                    _redis = ConnectionMultiplexer.Connect(RedisConStr);
                    _redis.ConfigurationChanged += _redisConfigurationChanged;
                    _redis.ConfigurationChangedBroadcast += _redisConfigurationChangedBroadcast;
                    _redis.ConnectionRestored += _redisConnectionRestored;
                    _redis.ConnectionFailed += _redisConnectionFailed;
                    _redis.ErrorMessage += _redisErrorMessage;
                    _redis.HashSlotMoved += _redisHashSlotMoved;
                    _redis.InternalError += _redisInternalError;
                    _initialized = true;
                }
            }
        }

        /// <summary>
        /// 反初始化
        /// </summary>
        public static void UnInitRedis()
        {
            lock (_locker)
            {
                if (_redis != null)
                {
                    _redisConStr = null;
                    _redis.ConfigurationChanged -= _redisConfigurationChanged;
                    _redis.ConfigurationChangedBroadcast -= _redisConfigurationChangedBroadcast;
                    _redis.ConnectionRestored -= _redisConnectionRestored;
                    _redis.ConnectionFailed -= _redisConnectionFailed;
                    _redis.ErrorMessage -= _redisErrorMessage;
                    _redis.HashSlotMoved -= _redisHashSlotMoved;
                    _redis.InternalError -= _redisInternalError;
                    _redis.Close();
                    _redis = null;
                    _initialized = false;
                }
            }
        }

        /// <summary>
        /// Redis ConnectionMultiplexer Instance
        /// </summary>
        public static ConnectionMultiplexer Redis
        {
            get
            {
                if (_redis == null || !_redis.IsConnected)
                {
                    InitRedis(RedisConStr);
                }

                return _redis;
            }
        }

        #region RedisEvents
        /// <summary>
        /// 重新建立连接之前的错误
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            Debug.WriteLine($"RedisConnectionRestored:{e.EndPoint}");
        }

        /// <summary>
        /// 通过发布订阅更新配置时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisConfigurationChangedBroadcast(object sender, EndPointEventArgs e)
        {
            Debug.WriteLine($"RedisConfigurationChangedBroadcast:{e.EndPoint}");
        }

        /// <summary>
        /// 配置更改时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisConfigurationChanged(object sender, EndPointEventArgs e)
        {
            Debug.WriteLine($"RedisConfigurationChanged:{e.EndPoint}");
        }

        /// <summary>
        /// redis类库错误
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisInternalError(object sender, InternalErrorEventArgs e)
        {
            Debug.WriteLine($"RedisInternalError:Message:{e.Exception.Message}");
        }

        /// <summary>
        /// 更改集群
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisHashSlotMoved(object sender, HashSlotMovedEventArgs e)
        {
            Debug.WriteLine($"RedisHashSlotMoved:NewEndPoint【{e.NewEndPoint}】OldEndPoint:【{e.OldEndPoint}】");
        }

        /// <summary>
        /// 发生错误时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisErrorMessage(object sender, RedisErrorEventArgs e)
        {
            Debug.WriteLine($"RedisErrorMessage:{e.Message}");
        }

        /// <summary>
        /// 连接失败 ， 如果重新连接成功你将不会收到这个通知
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _redisConnectionFailed(object sender, ConnectionFailedEventArgs e)
        {
            Debug.WriteLine($"RedisConnectionFailed:重新连接:Endpoint【{e.EndPoint}】,【{e.FailureType}】,【{(e.Exception == null ? string.Empty : e.Exception.Message)}】");
        }

        #endregion

        #endregion

        #region Key（键）完事儿

        /// <summary>
        /// KeyDelete
        /// </summary>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool KeyDelete(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.KeyDelete(key, flags);
        }

        /// <summary>
        /// KeyDelete
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long KeyDelete(string[] keys, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var keyArray = new RedisKey[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                keyArray[i] = keys[i];
            }
            return database.KeyDelete(keyArray, flags);
        }

        /// <summary>
        /// KeyDump
        /// </summary>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static byte[] KeyDump(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.KeyDump(key, flags);
        }

        /// <summary>
        /// KeyExists
        /// </summary>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool KeyExists(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.KeyExists(key);
        }

        /// <summary>
        /// KeyExists
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expiry"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool KeyExpire(string key, TimeSpan? expiry, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.KeyExpire(key, expiry, flags);
        }

        /// <summary>
        /// KeyExpire
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expiry"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool KeyExpire(string key, DateTime? expiry, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.KeyExpire(key, expiry, flags);
        }

        /// <summary>
        /// 模糊匹配查询Key
        /// </summary>
        /// <param name="hostandport"></param>
        /// <param name="pattern"></param>
        /// <param name="db"></param>
        /// <returns></returns>
        public static IEnumerable<RedisKey> Keys(string hostandport, string pattern, int db = 0)
        {
            return Redis.GetServer(hostandport).Keys(db, pattern);
        }

        //EXPIREAT          在哪里？
        //KeyMigrate
        //KeyMigrateAsync
        //KeyMove
        //KeyMoveAsync
        //OBJECT            在哪里？
        //KeyPersist
        //KeyPersistAsync
        //PEXPIRE           在哪里？
        //PEXPIREAT         在哪里？
        //PTTL              在哪里？
        //KeyRandom
        //KeyRandomAsync
        //KeyRename
        //KeyRenameAsync
        //RENAMENX          在哪里？
        //KeyRestore
        //KeyRestoreAsync
        //Sort
        //SortAsync
        //TTL               在哪里？
        //KeyType
        //KeyTypeAsync
        //SCAN              在哪里？
        #endregion

        #region Object

        /// <summary>
        /// Get
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T Get<T>(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return DeserializeObject<T>(database.StringGet(key, flags));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keys"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T[] Get<T>(string[] keys, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var redisKeys = new RedisKey[keys.Length];
            for (int i = 0; i < keys.Length; i++)
                redisKeys[i] = keys[i];
            var redisVals = database.StringGet(redisKeys, flags);
            var res = new T[redisVals.Length];
            for (int i = 0; i < redisVals.Length; i++)
                res[i] = DeserializeObject<T>(redisVals[i]);
            return res;
        }

        /// <summary>
        /// Set
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="timeSpan"></param>
        /// <param name="when"></param>
        /// <param name="flags"></param>
        public static void Set<T>(string key, T value, int db = 0, TimeSpan? timeSpan = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            database.StringSet(key, SerializeObject(value), timeSpan, when, flags);
        }

        /// <summary>
        /// Increment
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long Increment(string key, long value = 1, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.StringIncrement(key, value, flags);
        }

        /// <summary>
        /// Decrement
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long Decrement(string key, long value = 1, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.StringDecrement(key, value, flags);
        }

        #endregion

        #region String
        ///APPEND
        ///BITCOUNT
        ///BITOP
        ///BITFIELD
        ///DECR
        ///DECRBY
        ///GET
        ///GETBIT
        ///GETRANGE
        ///GETSET
        ///INCR
        ///INCRBY
        ///INCRBYFLOAT
        ///MGET
        ///MSET
        ///MSETNX
        ///PSETEX
        ///SET
        ///SETBIT
        ///SETEX
        ///SETNX
        ///SETRANGE
        ///STRLEN
        #endregion

        #region Hash完事儿
        /// <summary>
        /// HDel
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool HDel(string key, string hashid, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashDelete(key, hashid, flags);
        }

        /// <summary>
        /// HDel
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashids"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long HDel(string key, string[] hashids, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var keyArray = new RedisValue[hashids.Length];
            for (int i = 0; i < hashids.Length; i++)
            {
                keyArray[i] = hashids[i];
            }
            return database.HashDelete(key, keyArray, flags);
        }

        /// <summary>
        /// HExists
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool HExists(string key, string hashid, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashExists(key, hashid, flags);
        }

        /// <summary>
        /// HGet
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hashid"></param>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T HGet<T>(string key, string hashid, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return DeserializeObject<T>(database.HashGet(key, hashid, flags));
        }

        /// <summary>
        /// HGet
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="hashids"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static IDictionary<string, T> HGet<T>(string key, string[] hashids, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var hashFields = new RedisValue[hashids.Length];
            for (int i = 0; i < hashids.Length; i++)
            {
                hashFields[i] = hashids[i];
            }
            var redisValues = database.HashGet(key, hashFields, flags);
            var resDic = new Dictionary<string, T>();
            for (int i = 0; i < redisValues.Length; i++)
            {
                if (redisValues[i].HasValue)
                    resDic.Add(hashids[i], DeserializeObject<T>(redisValues[i]));
            }
            return resDic;
        }

        /// <summary>
        /// HGetAll
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static IDictionary<string, T> HGetAll<T>(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            HashEntry[] hashEntry = database.HashGetAll(key, flags);
            var resDic = new Dictionary<string, T>();
            foreach (var item in hashEntry)
            {
                resDic.Add(item.Name, DeserializeObject<T>(item.Value));
            }
            return resDic;
        }

        /// <summary>
        /// HIncrBy
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long HIncrBy(string key, string hashid, long value = 1, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashIncrement(key, hashid, value, flags);
        }

        /// <summary>
        /// HIncrByDouble
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static double HIncrByDouble(string key, string hashid, double value = 1.0, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashIncrement(key, hashid, value, flags);
        }

        /// <summary>
        /// HDecrBy
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long HDecrBy(string key, string hashid, long value = 1, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashDecrement(key, hashid, value, flags);
        }

        /// <summary>
        /// HDecrByDouble
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static double HDecrByDouble(string key, string hashid, double value = 1.0, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashDecrement(key, hashid, value, flags);
        }

        /// <summary>
        /// HKeys
        /// </summary>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static string[] HKeys(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var redisValues = database.HashKeys(key, flags);
            var res = new string[redisValues.Length];
            for (int i = 0; i < redisValues.Length; i++)
            {
                res[i] = redisValues[i];
            }
            return res;
        }

        /// <summary>
        /// HLen
        /// </summary>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long HLen(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashLength(key, flags);
        }

        /// <summary>
        /// HSet
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="hashid"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="when"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool HSet<T>(string key, string hashid, T value, int db = 0, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.HashSet(key, hashid, SerializeObject(value), when, flags);
        }

        /// <summary>
        /// HSet
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="dic"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        public static void HSet<T>(string key, IDictionary<string, T> dic, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var hashEntry = new HashEntry[dic.Count];
            int i = 0;
            foreach (var item in dic)
            {
                hashEntry[i++] = new HashEntry(item.Key, SerializeObject(item.Value));
            }
            database.HashSet(key, hashEntry, flags);
        }

        /// <summary>
        /// HValues
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T[] HValues<T>(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            var redisValues = database.HashValues(key, flags);

            var res = new T[redisValues.Length];
            for (int i = 0; i < redisValues.Length; i++)
            {
                res[i] = DeserializeObject<T>(redisValues[i]);
            }
            return res;
        }

        /// <summary>
        /// HScan
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="pattern"></param>
        /// <param name="db"></param>
        /// <returns></returns>
        public static T[] HScan<T>(string key, string pattern, int db = 0)
        {
            //TODO 需要测试
            var database = Redis.GetDatabase(db);
            var values = database.HashScan(key, pattern);
            var res = new T[values.Count()];
            int i = 0;
            foreach (var item in values)
            {
                res[i++] = DeserializeObject<T>(item.Value);
            }
            return res;
        }

        #endregion

        #region Set完事儿
        /// <summary>
        /// SAdd
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        public static void SAdd<T>(string key, T value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            database.SetAdd(key, SerializeObject(value), flags);
        }
        /// <summary>
        /// SAddAsync
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static Task<bool> SAddAsync<T>(string key, T value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.SetAddAsync(key, SerializeObject(value), flags);
        }
        /// <summary>
        /// Set元素数量
        /// </summary>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        public static long SCard(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.SetLength(key, flags);
        }

        /// <summary>
        /// 是否是Set成员
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool SIsMember<T>(string key, T value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.SetContains(key, SerializeObject(value));
        }

        ///SDIFF
        ///SDIFFSTORE
        ///SINTER
        ///SINTERSTORE
        ///SISMEMBER
        ///SMEMBERS
        ///SMOVE
        ///SPOP
        ///SRANDMEMBER
        ///SREM
        ///SUNION
        ///SUNIONSTORE
        ///SSCAN

        /// <summary>
        /// SPop
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T SPop<T>(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return DeserializeObject<T>(database.SetPop(key, flags));
        }
        /// <summary>
        /// SRemove
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static bool SRemove<T>(string key, T value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.SetRemove(key, SerializeObject(value), flags);
        }
        /// <summary>
        /// SRemoveAsync
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static Task<bool> SRemoveAsync<T>(string key, T value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.SetRemoveAsync(key, SerializeObject(value), flags);
        }

        #endregion

        #region List

        /// <summary>
        /// LPush
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="when"></param>
        /// <param name="flags"></param>
        public static void LPush<T>(string key, T value, int db = 0, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            database.ListLeftPush(key, SerializeObject(value), when, flags);
        }

        /// <summary>
        /// RPop
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T RPop<T>(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return DeserializeObject<T>(database.ListRightPop(key, flags));
        }

        /// <summary>
        /// RPush
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="when"></param>
        /// <param name="flags"></param>
        public static void RPush<T>(string key, T value, int db = 0, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            database.ListRightPush(key, SerializeObject(value), when, flags);
        }

        /// <summary>
        /// LPop
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static T LPop<T>(string key, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return DeserializeObject<T>(database.ListLeftPop(key, flags));
        }

        #endregion

        #region Subscriber

        /// <summary>
        /// GetSubscriber
        /// </summary>
        /// <returns></returns>
        public static ISubscriber GetSubscriber()
        {
            return Redis.GetSubscriber();
        }
        /// <summary>
        /// Publish
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long Publish<T>(string key, T value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.Publish(key, SerializeObject(value), flags);
        }
        /// <summary>
        /// PublishString
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="db"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public static long PublishString(string key, string value, int db = 0, CommandFlags flags = CommandFlags.None)
        {
            var database = Redis.GetDatabase(db);
            return database.Publish(key, value, flags);
        }


        #endregion

        #region Json
        private static T DeserializeObject<T>(string obj)
        {
            if (string.IsNullOrEmpty(obj))
                return default(T);
            return JsonConvert.DeserializeObject<T>(obj);
        }
        private static string SerializeObject(object obj)
        {
            if (obj == null)
                return null;
            return JsonConvert.SerializeObject(obj, TimeFormat);
        }
        #endregion
    }
}
