object MyHttpClient {

  def get(url: String) = scala.io.Source.fromURL(url).mkString

}
