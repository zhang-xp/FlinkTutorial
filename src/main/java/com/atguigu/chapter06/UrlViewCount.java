package com.atguigu.chapter06;

public class UrlViewCount {
    public String url;
    public Long count;
    public Long windowStart;
    public Long endStart;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getEndStart() {
        return endStart;
    }

    public void setEndStart(Long endStart) {
        this.endStart = endStart;
    }

    public UrlViewCount(String url, Long count, Long windowStart, Long endStart) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.endStart = endStart;
    }

    public UrlViewCount() {
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", endStart=" + endStart +
                '}';
    }
}
