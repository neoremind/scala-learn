

/**
 * 用scala重构报表服务里的一段代码
 *
 * @author zhangxu
 */
object FunctionalWayThinking {

  def main(args: Array[String]) {
    val itemList = List(StatInfo(1), StatInfo(2), StatInfo(3), StatInfo(5))
    println(fillSiteUrl(itemList))
  }

  def fillSiteUrl[T <: StatInfo](itemList: List[T]): List[StatInfo] = {
    val kvs = siteKvApi.query(itemList.map(_.k).toSet)
    itemList.map(i => StatInfo(i.k, kvs(i.k)))
  }

  class SiteKvApi {

    val kvs = Map(
      1 -> "abc",
      2 -> "uuu",
      3 -> "yyy",
      4 -> "ppp"
    )

    def query(keys: Set[Int]): Map[Int, String] = {
      keys.map(key => (key, kvs.getOrElse(key, "-"))).toMap
    }
  }

  object siteKvApi extends SiteKvApi

  case class StatInfo(k: Int, var v: String = "")

  //
  //  def fillSiteUrl[T](itemList: List[T]): List[T] {
  //    itemList.
  //    List (1)
  //  }

  //  /**
  //   * 填充item的url数据
  //   *
  //   * @param itemList
  //   * @return 2014-7-25 下午2:42:25 created by wangchongjie
  //   */
  //  public <T extends SiteNameAware> List<T> fillSiteUrl(List<T> itemList) {
  //    if (CollectionUtils.isEmpty(itemList)) {
  //      return itemList;
  //    }
  //    Set<BigInteger> keys = new HashSet<BigInteger>();
  //    for (T item : itemList) {
  //      keys.add(item.getOlapSiteId());
  //    }
  //
  //    Map<BigInteger, String> siteKV = siteKvApi.querySiteValue(keys);
  //    if (siteKV == null) {
  //      for (T item : itemList) {
  //        item.setSiteUrl("-");
  //      }
  //      LOG.warn("[literal] not found for [sign]: " + keys);
  //      return itemList;
  //    }
  //
  //    for (T item : itemList) {
  //      BigInteger id = item.getOlapSiteId();
  //      String site = siteKV.get(id);
  //      if (null == site) {
  //        item.setSiteUrl("-");
  //        LOG.warn("[literal] not found for [sign]: " + id);
  //      } else {
  //        item.setSiteUrl(site);
  //      }
  //    }
  //    return itemList;
  //  }

}

