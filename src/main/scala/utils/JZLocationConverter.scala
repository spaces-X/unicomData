package utils

object JZLocationConverter {
  private def LAT_OFFSET_0(x: Double, y: Double) = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x))

  private def LAT_OFFSET_1(x: Double, y: Double) = (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0

  private def LAT_OFFSET_2(x: Double, y: Double) = (20.0 * Math.sin(y * Math.PI) + 40.0 * Math.sin(y / 3.0 * Math.PI)) * 2.0 / 3.0

  private def LAT_OFFSET_3(x: Double, y: Double) = (160.0 * Math.sin(y / 12.0 * Math.PI) + 320 * Math.sin(y * Math.PI / 30.0)) * 2.0 / 3.0

  private def LON_OFFSET_0(x: Double, y: Double) = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x))

  private def LON_OFFSET_1(x: Double, y: Double) = (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0

  private def LON_OFFSET_2(x: Double, y: Double) = (20.0 * Math.sin(x * Math.PI) + 40.0 * Math.sin(x / 3.0 * Math.PI)) * 2.0 / 3.0

  private def LON_OFFSET_3(x: Double, y: Double) = (150.0 * Math.sin(x / 12.0 * Math.PI) + 300.0 * Math.sin(x / 30.0 * Math.PI)) * 2.0 / 3.0

  private val RANGE_LON_MAX = 137.8347
  private val RANGE_LON_MIN = 72.004
  private val RANGE_LAT_MAX = 55.8271
  private val RANGE_LAT_MIN = 0.8293
  private val jzA = 6378245.0
  private val jzEE = 0.00669342162296594323

  def transformLat(x: Double, y: Double): Double = {
    var ret = LAT_OFFSET_0(x, y)
    ret += LAT_OFFSET_1(x, y)
    ret += LAT_OFFSET_2(x, y)
    ret += LAT_OFFSET_3(x, y)
    ret
  }

  def transformLon(x: Double, y: Double): Double = {
    var ret = LON_OFFSET_0(x, y)
    ret += LON_OFFSET_1(x, y)
    ret += LON_OFFSET_2(x, y)
    ret += LON_OFFSET_3(x, y)
    ret
  }

  def outOfChina(lat: Double, lon: Double): Boolean = {
    if (lon < RANGE_LON_MIN || lon > RANGE_LON_MAX) return true
    if (lat < RANGE_LAT_MIN || lat > RANGE_LAT_MAX) return true
    false
  }

  def gcj02Encrypt(ggLat: Double, ggLon: Double): JZLocationConverter.LatLng = {
    val resPoint = new JZLocationConverter.LatLng
    var mgLat = .0
    var mgLon = .0
    if (outOfChina(ggLat, ggLon)) {
      resPoint.latitude = ggLat
      resPoint.longitude = ggLon
      return resPoint
    }
    var dLat = transformLat(ggLon - 105.0, ggLat - 35.0)
    var dLon = transformLon(ggLon - 105.0, ggLat - 35.0)
    val radLat = ggLat / 180.0 * Math.PI
    var magic = Math.sin(radLat)
    magic = 1 - jzEE * magic * magic
    val sqrtMagic = Math.sqrt(magic)
    dLat = (dLat * 180.0) / ((jzA * (1 - jzEE)) / (magic * sqrtMagic) * Math.PI)
    dLon = (dLon * 180.0) / (jzA / sqrtMagic * Math.cos(radLat) * Math.PI)
    mgLat = ggLat + dLat
    mgLon = ggLon + dLon
    resPoint.latitude = mgLat
    resPoint.longitude = mgLon
    resPoint
  }

  def gcj02Decrypt(gjLat: Double, gjLon: Double): JZLocationConverter.LatLng = {
    val gPt = gcj02Encrypt(gjLat, gjLon)
    val dLon = gPt.longitude - gjLon
    val dLat = gPt.latitude - gjLat
    val pt = new JZLocationConverter.LatLng
    pt.latitude = gjLat - dLat
    pt.longitude = gjLon - dLon
    pt
  }

  def bd09Decrypt(bdLat: Double, bdLon: Double): JZLocationConverter.LatLng = {
    val gcjPt = new JZLocationConverter.LatLng
    val x = bdLon - 0.0065
    val y = bdLat - 0.006
    val z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * Math.PI)
    val theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * Math.PI)
    gcjPt.longitude = z * Math.cos(theta)
    gcjPt.latitude = z * Math.sin(theta)
    gcjPt
  }

  def bd09Encrypt(ggLat: Double, ggLon: Double): JZLocationConverter.LatLng = {
    val bdPt = new JZLocationConverter.LatLng
    val x = ggLon
    val y = ggLat
    val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * Math.PI)
    val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * Math.PI)
    bdPt.longitude = z * Math.cos(theta) + 0.0065
    bdPt.latitude = z * Math.sin(theta) + 0.006
    bdPt
  }

  /**
    * @param location 世界标准地理坐标(WGS-84)
    * @return 中国国测局地理坐标（GCJ-02）<火星坐标>
    * @brief 世界标准地理坐标(WGS-84) 转换成 中国国测局地理坐标（GCJ-02）<火星坐标>
    *
    *        ####只在中国大陆的范围的坐标有效，以外直接返回世界标准坐标
    */
  def wgs84ToGcj02(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = gcj02Encrypt(location.latitude, location.longitude)

  /**
    * @param location 中国国测局地理坐标（GCJ-02）
    * @return 世界标准地理坐标（WGS-84）
    * @brief 中国国测局地理坐标（GCJ-02） 转换成 世界标准地理坐标（WGS-84）
    *
    *        ####此接口有1－2米左右的误差，需要精确定位情景慎用
    */
  def gcj02ToWgs84(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = gcj02Decrypt(location.latitude, location.longitude)

  /**
    * @param location 世界标准地理坐标(WGS-84)
    * @return 百度地理坐标（BD-09)
    * @brief 世界标准地理坐标(WGS-84) 转换成 百度地理坐标（BD-09)
    */
  def wgs84ToBd09(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = {
    val gcj02Pt = gcj02Encrypt(location.latitude, location.longitude)
    bd09Encrypt(gcj02Pt.latitude, gcj02Pt.longitude)
  }

  /**
    * @param location 中国国测局地理坐标（GCJ-02）<火星坐标>
    * @return 百度地理坐标（BD-09)
    * @brief 中国国测局地理坐标（GCJ-02）<火星坐标> 转换成 百度地理坐标（BD-09)
    */
  def gcj02ToBd09(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = bd09Encrypt(location.latitude, location.longitude)

  /**
    * @param location 百度地理坐标（BD-09)
    * @return 中国国测局地理坐标（GCJ-02）<火星坐标>
    * @brief 百度地理坐标（BD-09) 转换成 中国国测局地理坐标（GCJ-02）<火星坐标>
    */
  def bd09ToGcj02(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = bd09Decrypt(location.latitude, location.longitude)

  /**
    * @param location 百度地理坐标（BD-09)
    * @return 世界标准地理坐标（WGS-84）
    * @brief 百度地理坐标（BD-09) 转换成 世界标准地理坐标（WGS-84）
    *
    *        ####此接口有1－2米左右的误差，需要精确定位情景慎用
    */
  def bd09ToWgs84(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = {
    val gcj02 = bd09ToGcj02(location)
    gcj02Decrypt(gcj02.latitude, gcj02.longitude)
  }


  class LatLng() {
    var latitude = .0
    var longitude = .0

    def this(latitude: Double, longitude: Double) {
      this()
      this.latitude = latitude
      this.longitude = longitude
    }

    def getLatitude: Double = latitude

    def setLatitude(latitude: Double): Unit = {
      this.latitude = latitude
    }

    def getLongitude: Double = longitude

    def setLongitude(longitude: Double): Unit = {
      this.longitude = longitude
    }

    override def toString: String = {
      return this.longitude + "," + this.latitude;
    }
  }

}
