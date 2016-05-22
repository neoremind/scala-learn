import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

/**
 * @author zhangxu
 */
class MockTest extends FlatSpec with MockFactory {

  "PlayerService" should "get 2 players" in {
    val winner = Player(id = 222, nickname = "boris")
    val loser = Player(id = 333, nickname = "neo")

    val playerServiceMock = mock[PlayerService]

    (playerServiceMock.getAllPlayers _).expects().returns(List(winner, loser))

    (playerServiceMock.getPlayerById _).expects(winner.id).returns(winner)

    val all = playerServiceMock.getAllPlayers()
    all should have size 2

    val res = playerServiceMock.getPlayerById(winner.id)
    res should be(winner)
  }

  type PlayerId = Int

  case class Player(id: PlayerId, nickname: String)

  trait PlayerService {
    def getAllPlayers(): List[Player]

    def getPlayerById(id: Int): Player
  }

}
