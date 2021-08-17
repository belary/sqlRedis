package com.mgtv.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

    private static final char BRACKET_START = '{';

    private static final char BRACKET_END = '}';

    private static final Logger log = LoggerFactory.getLogger(StringUtils.class);


    public static String lowerCase(String s) {
       return s.toLowerCase();
    }

    /**
     * 检查指定的字符串是否为空。 <ul> <li>SysUtils.isEmpty(null) = true</li> <li>SysUtils.isEmpty("") =
     * true</li> <li>SysUtils.isEmpty("   ") = true</li> <li>SysUtils.isEmpty("abc") = false</li>
     * </ul>
     *
     * @param value 待检查的字符串
     * @return true/false
     */
    public static boolean isEmpty(String value) {
        int strLen;
        if (value == null || (strLen = value.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(value.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNotEmpty(String value) {
        return !isEmpty(value);
    }

    /**
     * 检查对象是否为数字型字符串,包含负数开头的。
     */
    public static boolean isNumeric(Object obj) {
        if (obj == null) {
            return false;
        }
        char[] chars = obj.toString().toCharArray();
        int length = chars.length;
        if (length < 1) {
            return false;
        }

        int i = 0;
        if (length > 1 && chars[0] == '-') {
            i = 1;
        }

        for (; i < length; i++) {
            if (!Character.isDigit(chars[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 检查指定的字符串列表是否不为空。
     */
    public static boolean areNotEmpty(String... values) {
        boolean result = true;
        if (values == null || values.length == 0) {
            result = false;
        } else {
            for (String value : values) {
                result &= !isEmpty(value);
            }
        }
        return result;
    }

    /**
     * 把通用字符编码的字符串转化为汉字编码。
     */
    public static String unicodeToChinese(String unicode) {
        StringBuilder out = new StringBuilder();
        if (!isEmpty(unicode)) {
            for (int i = 0; i < unicode.length(); i++) {
                out.append(unicode.charAt(i));
            }
        }
        return out.toString();
    }

    /**
     * 将驼峰字符串转为下划线分隔的字符串
     */
    public static String toUnderlineStyle(String name) {
        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    newName.append("_");
                }
                newName.append(Character.toLowerCase(c));
            } else {
                newName.append(c);
            }
        }
        return newName.toString();
    }

    public static String toString(byte[] bytes, String charset) {
        return toString(bytes, 0, bytes.length, charset);
    }

    public static String toString(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    public static String toString(byte[] data, int offset, int length, String charset) {
        if (data == null) {
            return null;
        }
        try {
            return new String(data, offset, length, charset);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static byte[] toBytes(String data, String charset) {
        if (data == null) {
            return null;
        }
        try {
            return data.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static boolean isDouble(String arg0) {
        if (arg0 == null || arg0.length() == 0) {
            return false;
        }

        try {
            Double.parseDouble(arg0);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 如果arg为null或转化为小定时为"null", 则返回"", 否则返回本身
     */
    public static String nullToEmpty(String arg) {
        return arg == null || "null".equals(arg.toLowerCase()) ? "" : arg;
    }

    /**
     * 将字符串数组中的元素中间使用separator分隔连接为一个字符串
     */
    public static String joinString(String[] array, String separator) {
        return joinString(Arrays.asList(array), separator);
    }


    public static String joinString(List<String> list, String separator) {
        return joinString(list.iterator(), separator);
    }

    public static String joinLong(List<Long> list, String separator) {
        return joinString(list.iterator(), separator);
    }

    public static String joinInteger(List<Integer> list, String separator) {
        return joinString(list.iterator(), separator);
    }

    /**
     * 将字符串Collection中的元素中间使用separator分隔连接为一个字符串
     */
    public static String joinString(Collection<String> coll, String separator) {
        return joinString(coll.iterator(), separator);
    }

    /**
     * 将字符串Collection中的元素中间使用separator分隔连接为一个字符串
     */
    public static String joinString(Iterator<?> list, String separator) {
        StringBuilder strBuilder = new StringBuilder();
        while (list.hasNext()) {
            if (strBuilder.length() > 0) {
                strBuilder.append(separator);
            }
            strBuilder.append(list.next());
        }
        return strBuilder.toString();
    }

    public static String[] split(String str, String separator) {
        List<String> list = splitToList(str, separator);
        return list.toArray(new String[list.size()]);
    }

    public static List<String> splitToList(String str, String separator) {
        List<String> list = new ArrayList<String>();
        if (isEmpty(str)) {
            return list;
        }

        if (isEmpty(separator)) {
            list.add(str);
            return list;
        }
        int lastIndex = -1;
        int index = str.indexOf(separator);
        if (-1 == index) {
            list.add(str);
            return list;
        }
        while (index >= 0) {
            if (index > lastIndex) {
                list.add(str.substring(lastIndex + 1, index));
            } else {
                list.add("");
            }

            lastIndex = index;
            index = str.indexOf(separator, index + 1);
            if (index == -1) {
                list.add(str.substring(lastIndex + 1, str.length()));
            }
        }
        return list;
    }

    public static String lpad(String str, int length, String pad) {
        while (str.length() < length) {
            str = pad + str;
        }
        return str;
    }

    public static String rpad(String str, int length, String pad) {
        while (str.length() < length) {
            str = str + pad;
        }
        return str;
    }

    /**
     * 首字母大写
     */
    public static String firstCharacterToUpper(String srcStr) {
        return srcStr.substring(0, 1).toUpperCase() + srcStr.substring(1);
    }

    /**
     * 去掉最后一位
     */
    public static String cutLast(String srcStr) {
        return srcStr.substring(0, srcStr.length() - 1);
    }

    /**
     * 替换字符串并让它的下一个字母为大写
     */
    public static String replaceUnderlineAndfirstToUpper(String srcStr,
        String org, String ob) {
        String newString = "";
        int first = 0;
        while (srcStr.indexOf(org) != -1) {
            first = srcStr.indexOf(org);
            if (first != srcStr.length()) {
                newString = newString + srcStr.substring(0, first) + ob;
                srcStr = srcStr
                    .substring(first + org.length(), srcStr.length());
                srcStr = firstCharacterToUpper(srcStr);
            }
        }
        newString = newString + srcStr;
        return newString;
    }


    public static boolean startsWith(String str, String prefix) {
        if (str == null || prefix == null) {
            return false;
        }
        if (str.startsWith(prefix)) {
            return true;
        }
        if (str.length() < prefix.length()) {
            return false;
        }
        String lcStr = str.substring(0, prefix.length());
        return lcStr.equals(prefix);
    }

    public static boolean isBigChar(char c) {
        return c < 0 || c > 256;
    }

    public static String trim(String arg) {
        return arg == null ? null : arg.trim();
    }

    /**
     * 浮点数运算,小数位4位
     *
     * @param type 运算类型：+：add,-:sub, /:divide, *:multiply
     */
    public static double comDouble(Double v1, Double v2, char type) {
        return comDouble(v1, v2, type, 4);
    }

    /**
     * 浮点数运算, 四舍五入
     *
     * @param type 运算类型：+：add,-:sub, /:divide, *:multiply
     * @param scale 小数点位数
     */
    public static double comDouble(Double v1, Double v2, char type, int scale) {
        BigDecimal ret = new BigDecimal("0.0");
        if (v1 != null) {
            ret = ret.add(new BigDecimal(v1.toString()));
        }
        if (v2 != null) {
            switch (type) {
                case '+':
                    ret = ret.add(new BigDecimal(v2.toString()));
                    break;
                case '-':
                    ret = ret.subtract(new BigDecimal(v2.toString()));
                    break;
                case '/':
                    ret = ret.divide(new BigDecimal(v2.toString()), 10, BigDecimal.ROUND_HALF_EVEN);
                    break;
                case '*':
                    ret = ret.multiply(new BigDecimal(v2.toString()));
                    break;
                default:
                    break;
            }
        }
        return ret.setScale(scale, BigDecimal.ROUND_HALF_EVEN).doubleValue();
    }

    /**
     * 判断src中是否包含args中的任何一个
     */
    public static boolean contains(String src, String... args) {
        if (args.length == 0) {
            return false;
        }

        for (String arg : args) {
            if (contain(src, arg)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contain(String src, String arg) {
        if (src == null) {
            return arg == null;
        }
        return arg != null ? src.contains(arg) : false;
    }

    /**
     * 字符串顺序反转
     *
     * @param str, 会执行trim
     */
    public static String reverseString(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        str = str.trim();
        int strl = str.length();
        if (strl > 1) {
            StringBuffer sb = new StringBuffer(str);
            return sb.reverse().toString();
        }
        return str;
    }

    /**
     * <pre>
     * 格式化数值，将科学记数法的数据转换成，
     * 并进行四舍五入，如果传入字符串为空则返回空
     * </pre>
     *
     * @param v 待格式化的字符串
     * @param scale 精度
     * @return double 格式化后数字
     */
    public static double setScale(double v, int scale) {
        if (scale < 0) {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        }

        BigDecimal b = new BigDecimal(v);
        b = b.setScale(scale, BigDecimal.ROUND_HALF_UP);
        return b.doubleValue();
    }

    /**
     * 拼装方法中参数
     */
    public static String getStringArgs(Object... args) {
        StringBuilder str = new StringBuilder();
        for (Object arg : args) {
            str.append(toString(arg));
            str.append(",");
        }
        return str.toString();
    }


    public static Map<String, String> getQueryMap(String query) {
        Map<String, String> map = new HashMap<>();

        if (query != null) {

            String[] params = query.split("&");
            for (String param : params) {
                String[] ret = param.split("=");
                if (ret.length == 2) {
                    String name = param.split("=")[0];
                    String value = param.split("=")[1];
                    map.put(name, value);
                }
            }
        }

        return map;
    }

    /**
     * 在指定的字符串固定长度插入指定分隔符，每3位一段，用空格分隔开的字符串 <p> eg: input: data      = 1234567890 sectionLen = 3
     * separator = - return:123-456-789-0
     */
    public static String splitByFix(String data, int sectionLen, String separator) {
        if (isEmpty(data)) {
            return data;
        }

        int secs = (data.length() + sectionLen - 1) / sectionLen;

        int x = 0;
        StringBuilder strBuilder = new StringBuilder(data);
        for (int i = 1; i < secs; i++) {
            int idx = i * sectionLen + x;

            strBuilder.insert(idx, separator);

            x += separator.length();
        }
        return strBuilder.toString();
    }

    /**
     * 占位符替换 如:StringUtil.format("{domain}:{port}", "http://www.baidu.com", 80)
     */
    public static String format(String format, Object... args) {
        StringBuilder sb = new StringBuilder();
        StringBuilder specifier = new StringBuilder();
        try {
            char[] ch = format.toCharArray();
            int num = 0;
            for (char c : ch) {
                if (c == BRACKET_START) {
                    specifier.append(c);
                    continue;
                }
                if (org.apache.commons.lang3.StringUtils.isNoneBlank(specifier)
                    && c != BRACKET_END) {
                    specifier.append(c);
                    continue;
                }
                if (c == BRACKET_END) {
                    specifier.append(c);
                    sb.append(args[num++]);
                    specifier = new StringBuilder();
                    continue;
                }
                sb.append(c);
            }

            // 没找到'}'
            if (org.apache.commons.lang3.StringUtils.isNoneBlank(specifier)) {
                throw new IllegalArgumentException("can not find '}'");
            }

            return sb.toString();
        } catch (Exception ex) {
            throw new MissingFormatArgumentException(specifier.toString());
        }
    }


    /**
     * sharding切分策略
     */
    public static int shardingIndex(String str, int shardingNum) {
        return Math.abs((str.hashCode()) % shardingNum);
    }

    /**
     * 创建一个整形区间的数组
     */
    public static int[] intRange(int start, int end, int increment) {
        if (start < end && increment < 0) {
            throw new IllegalArgumentException();
        }
        if (start > end && increment > 0) {
            throw new IllegalArgumentException();
        }

        int[] values = new int[Math.abs((end - start) / increment) + 1];
        boolean reverse = start > end;

        for (int i = start, index = 0; reverse ? (i >= end) : (i <= end); i += increment, ++index) {
            values[index] = i;
        }
        return values;
    }

    /**
     * 创建一个字符型区间的数组
     */
    public static String[] stringRange(int start, int end, int increment) {
        if (start < end && increment < 0) {
            throw new IllegalArgumentException();
        }
        if (start > end && increment > 0) {
            throw new IllegalArgumentException();
        }

        String[] values = new String[Math.abs((end - start) / increment) + 1];
        boolean reverse = start > end;

        for (int i = start, index = 0; reverse ? (i >= end) : (i <= end); i += increment, ++index) {
            values[index] = String.valueOf(i);
        }
        return values;
    }

    /**
     * 返回字符串首字母ASCII值
     */
    public static int ord(String s) {
        return s.length() > 0 ? (s.getBytes(StandardCharsets.UTF_8)[0] & 0xff) : 0;
    }

    /**
     * 返回字符首字母ASCII值
     */
    public static int ord(char c) {
        return c < 0x80 ? c : ord(Character.toString(c));
    }


    /**
     * 解析对象异常则为0
     * @param obj
     * @return
     */
    public static int parseInt(Object obj) {
        try {
            return Integer.parseInt(obj.toString());
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 解析对象字符串异常则为0
     * @param string
     * @return
     */
    public static int parseInt(String string) {
        try {
            return Integer.parseInt(string);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 解析对象异常则为0
     * @param obj
     * @return
     */
    public static long parseLong(Object obj) {
        try {
            return Long.parseLong(obj.toString());
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 解析字符串异常则为0
     * @param string
     * @return
     */
    public static long parseLong(String string) {
        try {
            return Long.parseLong(string);
        } catch (Exception e) {
            return 0;
        }
    }


    /**
     * 序列异常堆栈信息
     * 2018/3/8 11:20
     */
    public static String exceptionInfo(Exception e) {

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        try {
            // 将出错的栈信息输出到printWriter中
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
        }finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (pw != null) {
                pw.close();
            }
        }
        return sw.toString();
    }

    /**
     * 字符串是否包含中文
     *
     * @param str 待校验字符串
     * @return true 包含中文字符 false 不包含中文字符
     */
    public static boolean isContainChinese(String str) {

        if (StringUtils.isEmpty(str)) {
            return false;
        }
        Pattern p = Pattern.compile("[\u4E00-\u9FA5|\\！|\\，|\\。|\\（|\\）|\\《|\\》|\\“|\\”|\\？|\\：|\\；|\\【|\\】]");
        Matcher m = p.matcher(str);
        return m.find();
    }

    public static String validateUrlHost(String strUrl) {
        String urlPath = "";
        try {
            urlPath = new URL(strUrl).getHost();
        } catch (Exception ex) {
            log.error("invalid url for host:"+strUrl);
        }
        return urlPath;
    }

    public static String validateUrlPath(String strUrl) {
        String urlPath = "";
        try {
            urlPath = new URL(strUrl).getPath();
        } catch (Exception ex) {
             log.error("invalid url for path:"+strUrl);
        }
        return urlPath;
    }

    /**
     * idx 需要改为两个idx直接的差值
     * @param playTime
     * @param idx
     * @return
     */
    public static int calcHeartbeatIdx(int playTime, int idx) {

        int htSec = 0;
        if (idx < 0) {
            return 0;
        } else if (idx > 6) {
            return 120;
        }
        switch (idx) {
            case 0:
                htSec = playTime;
                break;
            case 1:
                htSec = 3;
                break;
            case 2:
                htSec = 2; // 5s
                break;
            case 3:
                htSec = 10; // 15s
                break;
            case 4:
                htSec = 30; // 45s
                break;
            case 5:
                htSec = 15; // 60s
                break;
            case 6:
                htSec = 60; // 120
                break;
            default:
                htSec = 0;
                break;
        }
        return  htSec;

    }

    public static void main(String[] args) throws MalformedURLException {
        String testUrls = new URL("https://ecom.mgtv.com/shop/detail/1559.html?ishidenavbar=1&pagename=goods_details&fdparams=&goodsid=1559")
                .getPath();
        System.out.println(testUrls);
    }

}

