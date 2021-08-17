package com.mgtv.data.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class MyParameterParser {

    private final Logger log = LoggerFactory.getLogger(MyParameterParser.class);

    public ParameterTool getParameter(String runningAt) {
        try {
            //兼容处理，若未传runat参数则获取原配置
            if (StringUtils.isNotEmpty(runningAt) ) {
                    return ParameterTool
                            .fromPropertiesFile(
                                    MyParameterParser.class.getResourceAsStream("/config."+ runningAt + ".properties")
                            );

            } else {
                return ParameterTool
                        .fromPropertiesFile(
                                MyParameterParser.class.getResourceAsStream("/config.properties")
                        );
            }
        } catch (IOException e) {
            log.error("配置文件解析异常", e);
        }
        return null;
    }


    @Deprecated
    public Configuration getConfiguration(String runningAt) {
        ParameterTool parameterTool = getParameter(runningAt);
        if (parameterTool != null) {
            Configuration conf = parameterTool.getConfiguration();
            conf.setString("runat", StringUtils.lowerCase(runningAt));
            return conf;
        }
        return null;
    }


    public static String timeStamp2Date(String seconds, String format) {

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds+"000")));
    }


    public static void main(String[] args) {

        String showField = "zl_id";
        showField = showField.substring(0, 1).toUpperCase() + showField.substring(1);
        System.out.println(showField);
    }

}
