package org.simon.project.mobileAppLog;

import java.io.Serializable;

/**
 * 访问日志信息类(必须可序列化 否则会报错)
 * 
 * @author simon
 *
 */
public class AccessLogInfo implements Serializable {

	private static final long serialVersionUID = 264173875936623330L;

	private long timestamp; // 时间戳
	private long upTraffic;// 上行流量
	private long downTraffic;// 下行流量

	// 必须要提供空的构造函数 用于反射创建对象
	public AccessLogInfo() {}

	public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
		super();
		this.timestamp = timestamp;
		this.upTraffic = upTraffic;
		this.downTraffic = downTraffic;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getUpTraffic() {
		return upTraffic;
	}

	public void setUpTraffic(long upTraffic) {
		this.upTraffic = upTraffic;
	}

	public long getDownTraffic() {
		return downTraffic;
	}

	public void setDownTraffic(long downTraffic) {
		this.downTraffic = downTraffic;
	}

}
