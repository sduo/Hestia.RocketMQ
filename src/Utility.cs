using Aliyun.MQ.Model;
using Hestia.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Hestia.RocketMQ
{
    public class Utility
    {
        internal const string DefaultFormatPrefix = "Format:";
        internal const string DefaultCharsetPrefix = "Charset:";
        internal const string DefaultFormat = "h16";

        private static readonly string[] formatRaw = new string[] { "raw", "none" };
        private static readonly string[] formatBase64 = new string[] { "b64", "base64" };
        private static readonly string[] formatHex = new string[] { "h16", "hex" };
        private static readonly string supportedFormats = string.Join(';' ,$"RAW:{string.Join(',', formatRaw)}", $"Base64:{string.Join(',', formatBase64)}", $"Hex:{string.Join(',', formatHex)}");

        private static readonly long maxDeliverOffset = 259200L; // 72*3600

        private const string propertyOrginId = "_OrginId";
        private const string propertyChainId = "_ChainId";
        private const string propertyOrginPublishTime = "_OrginPublishTime";
        private const string propertyTotalConsumedTimes = "_TotalConsumedTimes";


        internal static readonly Dictionary<string, Func<string, string>> PublishMessagePropertyMapper = new() {
            { "KEYS",x=>null },
            { "__SHARDINGKEY",x=>null },
            { "__STARTDELIVERTIME",x=>null }
        };

        internal static readonly Dictionary<string, Func<string, string>> RetryMessagePropertyMapper = new() {
            { "KEYS",x=>null },
            { "__SHARDINGKEY",x=>null },
            { "__STARTDELIVERTIME",x=>null },
            { "__BORNHOST",x=>null }
        };

        internal static readonly Dictionary<string, Func<string, string>> ConsumeMessagePropertyMapper = new() {
            { "KEYS",x=>null },
            { "__SHARDINGKEY",x=>null },
            { "__BORNHOST" , x=>x },
            { "__STARTDELIVERTIME",x=> GetDateTimeString(x?.ToLong()) }
        };

        internal static readonly Dictionary<string, Func<Message, string>> RetryMessagePropertyInjector = new() {
            { propertyOrginId,x=>x.GetProperty(propertyOrginId,x.Id) },
            { propertyChainId,x=>x.Id },
            { propertyOrginPublishTime,x=> x.GetProperty(propertyOrginPublishTime,$"{x.PublishTime}") },
            { propertyTotalConsumedTimes,x=> $"{(x.GetProperty(propertyTotalConsumedTimes)?.ToUnsignedInt() ?? x.ConsumedTimes)+1u}" }
        };

        internal static readonly Dictionary<string, Func<Message, string>> ConsumeMessageSdkPropertyInjector = new()
        {
            {$"__{nameof(Message.ReceiptHandle)}",x=>x.ReceiptHandle },
            {$"__{nameof(Message.BodyMD5)}",x=>x.BodyMD5 },
            {$"__{nameof(Message.ConsumedTimes)}",x=>$"{x.ConsumedTimes}" },
            {$"__{nameof(Message.FirstConsumeTime)}",x=> GetDateTimeString(x.FirstConsumeTime) },
            {$"__{nameof(Message.NextConsumeTime)}",x=>GetDateTimeString(x.NextConsumeTime) },
            {$"__{nameof(Message.PublishTime)}",x=>GetDateTimeString(x.PublishTime)}
        };
        internal static readonly Dictionary<string, Func<Message, string>> ConsumeMessagePropertyInjector = new() {
            { propertyOrginId,x=>x.GetProperty(propertyOrginId,x.Id) },
            { propertyChainId,x=>x.GetProperty(propertyChainId,x.Id) },
            { propertyOrginPublishTime,x=> GetDateTimeString(x.GetProperty(propertyOrginPublishTime,null)?.ToLong() ?? x.PublishTime) },
            { propertyTotalConsumedTimes,x=> x.GetProperty(propertyTotalConsumedTimes,$"{x.ConsumedTimes}") }
        };


        internal static string GetDateTimeString(long? timestamp)
        {
            if (!timestamp.HasValue) { return null; }
            return $"{DateTimeOffset.FromUnixTimeMilliseconds(timestamp.Value).LocalDateTime:yyyy-MM-dd HH:mm:ss.fff}";
        }        

        internal static void VerifyDelayInRange(long delay)
        {
            if (delay <= 0 || delay > maxDeliverOffset) 
            { 
                throw new ArgumentOutOfRangeException(nameof(delay), $"{nameof(delay)}: (0,{maxDeliverOffset}]"); 
            }
        }        

        private static Func<byte[], string> GetEncoder(string name)
        {
            if (formatRaw.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                return null;
            }
            if (formatBase64.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                return Convert.ToBase64String;
            }
            if (formatHex.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                return Convert.ToHexString;
            }
            throw new ArgumentException(supportedFormats, nameof(name));
        }

        private static Func<string, byte[]> GetDecoder(string name)
        {
            if (formatRaw.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                return null;
            }
            if (formatBase64.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                return Convert.FromBase64String;
            }
            if (formatHex.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                return Convert.FromHexString;
            }
            throw new ArgumentException(supportedFormats, nameof(name));
        }

        private static bool EncodingFilter(EncodingInfo encoding, string name)
        {
            return string.Equals(encoding.Name, name, StringComparison.OrdinalIgnoreCase)
                || string.Equals(encoding.DisplayName, name, StringComparison.OrdinalIgnoreCase);
        }

        internal static Encoding GetEncoding(string name)
        {
            if (string.IsNullOrEmpty(name)) { return null; }
            return Encoding.GetEncodings().FirstOrDefault(x => EncodingFilter(x, name))?.GetEncoding();
        }

        internal static string GetFromDictionary(IDictionary<string, string> source, string key, string @default)
        {
            if (source is null) { return @default; }
            if (key is null) { return @default; }
            var target = source.Keys.FirstOrDefault(x=> string.Equals(x,key,StringComparison.OrdinalIgnoreCase));
            if(target is null) { return @default; }
            return source[key]; 
        }

        internal static string Transform(string source, Func<string> format,Func<string> charset, Func<string, string, string, string> codec)
        {
            if (source is null) { return null; }            
            return codec(source, format.Invoke(), charset.Invoke());
        }

        internal static string Encode(string source, string encoder, string encoding = null)
        {
            return Encode(source, encoder ?? DefaultFormat, GetEncoding(encoding) ?? Encoding.UTF8);
        }

        internal static string Encode(string source, string encoder, Encoding encoding)
        {
            return source.Transform(encoding.GetBytes).Transform(GetEncoder(encoder));
        }

        internal static string Decode(string source, string decoder, string encoding = null)
        {
            return Decode(source, decoder ?? DefaultFormat, GetEncoding(encoding) ?? Encoding.UTF8);
        }

        internal static string Decode(string source, string decoder, Encoding encoding)
        {
            return source.Transform(GetDecoder(decoder)).Transform(encoding.GetString);
        }
    }
}
