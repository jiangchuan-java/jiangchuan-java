package com.ifeng.rocketmq.connect.util;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;
import java.util.Objects;

/**
 * http请求工具类
 * <p>
 * Created by licheng1 on 2016/12/2.
 */
public class HttpUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

    public static final String DEFAULT_CHARSET = "UTF-8";

    private static final String URL_TYPE_HTTPS = "https";

    public static final Integer DEFAULT_SOCKET_TIMEOUT = 5000;

    public static final Integer DEFAULT_CONNECT_TIMEOUT = 5000;

    public static final Integer DEFAULT_CONNECTION_REQUEST_TIMEOUT = 5000;

    private static final CloseableHttpClient SYNC_CLIENT;

    //private static final CloseableHttpAsyncClient ASYNC_CLIENT = HttpAsyncClients.createDefault();

    static {
//        SYNC_CLIENT = HttpClients.createDefault();
        SYNC_CLIENT = HttpClients.custom()
                .setMaxConnTotal(300)
                .setMaxConnPerRoute(60)
                .build();
    }

    public static String post(String url, String params) throws Exception {
        return post(url, params, DEFAULT_CHARSET);
    }

    public static String post(String url, String params, String charset) throws Exception {
        return post(url, params, charset, null, DEFAULT_SOCKET_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
    }

    public static String post(String url, String params, Map<String, String> headers) throws Exception {
        return post(url, params, DEFAULT_CHARSET, headers, DEFAULT_SOCKET_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
    }

    public static String post(String url, String params, int socketTimeout, int connectTimeout, int connectionRequestTimeout) throws Exception {
        return post(url, params, DEFAULT_CHARSET, null, DEFAULT_SOCKET_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
    }

    public static String get(String url) throws Exception {
        return get(url, null);
    }

    public static String get(String url, String params) throws Exception {
        return get(url, params, DEFAULT_CHARSET);
    }

    public static String get(String url, String params, String charset) throws Exception {
        return get(url, params, charset, null, DEFAULT_SOCKET_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
    }

    public static String get(String url, String params, Map<String, String> headers) throws Exception {
        return get(url, params, DEFAULT_CHARSET, headers, DEFAULT_SOCKET_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
    }

    public static String get(String url, String params, int socketTimeout, int connectTimeout, int connectionRequestTimeout) throws Exception {
        return get(url, params, DEFAULT_CHARSET, null, DEFAULT_SOCKET_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
    }

    /**
     * 发送get请求
     *
     * @param url                      请求地址
     * @param params                   请求参数
     * @param charset                  字符集
     * @param socketTimeout            socket超时
     * @param connectTimeout           connect超时
     * @param connectionRequestTimeout connectionRequest超时
     * @return 响应结果
     * @throws Exception 连接异常
     */
    public static String get(String url, String params, String charset, Map<String, String> headers, int socketTimeout, int connectTimeout, int connectionRequestTimeout) throws Exception {
        Objects.requireNonNull(url, "http get url is null");

        if (Objects.isNull(charset) || "".equals(charset))
            charset = DEFAULT_CHARSET;

        RequestConfig conf = RequestConfig.custom()
                .setSocketTimeout(socketTimeout)
                .setConnectTimeout(connectTimeout)
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .build();


        url = getUrl(url, params);
        HttpGet get = new HttpGet(url);

        get.setConfig(conf);
        if (Objects.nonNull(headers) && !headers.isEmpty())
            addHeaders(get, headers);

        CloseableHttpResponse response = SYNC_CLIENT.execute(get);

        try {
            String resp = null;
            if (response.getStatusLine().getStatusCode() == 200) {
                resp = EntityUtils.toString(response.getEntity(), charset);
            }

            return resp;
        } finally {
            if (response != null)
                response.close();
        }
    }

    /**
     * 发送post请求
     *
     * @param url                      请求地址
     * @param params                   请求参数
     * @param charset                  字符集
     * @param socketTimeout            socket超时
     * @param connectTimeout           connect超时
     * @param connectionRequestTimeout connectionRequest超时
     * @return 响应结果
     * @throws Exception 连接异常
     */
    public static String post(String url, String params, String charset, Map<String, String> headers, int socketTimeout, int connectTimeout, int connectionRequestTimeout) throws Exception {
        Objects.requireNonNull(url, "http post url is null");
        Objects.requireNonNull(params, "http post params is null");

        if (Objects.isNull(charset) || "".equals(charset))
            charset = DEFAULT_CHARSET;

        RequestConfig conf = RequestConfig.custom()
                .setSocketTimeout(socketTimeout)
                .setConnectTimeout(connectTimeout)
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .build();

        HttpPost post = new HttpPost(url);
        StringEntity entity = new StringEntity(params, charset);
        post.setEntity(entity);
        post.setConfig(conf);
        if (Objects.nonNull(headers) && !headers.isEmpty())
            addHeaders(post, headers);

        CloseableHttpResponse response = null;
        CloseableHttpClient httpsClient = null;
        int statusCode = 0;
        try {
            URL httpUrl = new URL(url);
            if (URL_TYPE_HTTPS.equals(httpUrl.getProtocol())) {
                httpsClient = new SSLClient();
                response = httpsClient.execute(post);
            } else {
                response = SYNC_CLIENT.execute(post);
            }
            String resp = null;
            if (response != null) {
                statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    resp = EntityUtils.toString(response.getEntity(), charset);
                } else {
                    throw new Exception("response statusCode not 200 url:"+url+" statusCode:"+statusCode+" param:"+params);
                }
            }

            return resp;
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (response != null)
                response.close();
            if (httpsClient != null)
                httpsClient.close();
        }
    }

    public static void addHeaders(HttpRequestBase request, Map<String, String> headers) {

        for (Map.Entry<String, String> header : headers.entrySet()) {
            request.addHeader(header.getKey(), header.getValue());
        }
    }

    public static String noQueryString(String url) {
        Objects.requireNonNull(url, "http request url cannot be null");

        int index = url.indexOf("?");

        if (index > 0) {
            url = url.substring(0, index);
        }
        return url;
    }

    public static String getUrl(String url, String params) {
        Objects.requireNonNull(url, "http request url is null");

        if (Objects.isNull(params) || Objects.equals(params, ""))
            return url;

        int index = url.indexOf("?");
        if (index > 0) {
            if (index == url.length() - 1)
                url = url.concat(params);
            else
                url = url.concat("&").concat(params);
        } else {
            url = url.concat("?").concat(params);
        }
        return url;
    }

    public static void main(String[] args) throws Exception {
        String url = "www.baidu.com";
        HttpUtils.get(url, "", "utf-8", null, 5000, 5000, 5000);
    }
}
