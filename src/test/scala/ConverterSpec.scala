import java.nio.file.{Files, Path, Paths}

import cats.effect.IO
import org.scalatest.FunSpec

class ConverterSpec extends FunSpec {
  describe("Converter") {

    val basePath = "src/test/resources"

    def celsiusToFahrenheit(in: Path, out: Path): IO[Unit] =
      Converter.converter(in, out).compile.drain

    it("should convert from Fahrenheit to Celsius") {
      val in  = Paths.get(basePath, "fahrenheit.txt")
      val out = Files.createTempFile("output", "txt")

      celsiusToFahrenheit(in, out).unsafeRunSync()

      val res = Files.lines(out).reduce("", (t: String, u: String) => t ++ u)
      val expected =
        Files.lines(Paths.get(basePath, "celsius.txt")).reduce("", (t: String, u: String) => t ++ u)
      assert(res === expected)
    }
  }
}
