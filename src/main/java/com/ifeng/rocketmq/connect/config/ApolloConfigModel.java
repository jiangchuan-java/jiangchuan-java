package com.ifeng.rocketmq.connect.config;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-8
 */
public class ApolloConfigModel {

    //状态
    private String state;

    //推送url
    private String httpSinkUrl;

    //重试次数
    private int remainRetries;

    //请求类型
    private String contentType;

    private long retryInertval;

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getHttpSinkUrl() {
        return httpSinkUrl;
    }

    public void setHttpSinkUrl(String httpSinkUrl) {
        this.httpSinkUrl = httpSinkUrl;
    }


    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public int getRemainRetries() {
        return remainRetries;
    }

    public void setRemainRetries(int remainRetries) {
        this.remainRetries = remainRetries;
    }

    public long getRetryInertval() {
        return retryInertval;
    }

    public void setRetryInertval(long retryInertval) {
        this.retryInertval = retryInertval;
    }
}
