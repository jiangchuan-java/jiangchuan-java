package com.ifeng.rocketmq.connect.core;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-8
 */
public class ApolloConfigModel {

    //状态
    private int state;

    //推送url
    private String httpSinkUrl;

    //重试次数
    private int retryTimes;

    //请求类型
    private String contentType;


    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getHttpSinkUrl() {
        return httpSinkUrl;
    }

    public void setHttpSinkUrl(String httpSinkUrl) {
        this.httpSinkUrl = httpSinkUrl;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

}
