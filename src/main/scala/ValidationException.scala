case class ValidationException(msg: String) extends Throwable {
  override def getMessage: String = msg
}
