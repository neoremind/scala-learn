package programmingpattern.p11

/**
  * 替代依赖注入
  */
object Example extends App {

  case class Movie(movieId: String, title: String)

  case class Video(movieId: String)

  case class DecoratedMovie(movie: Movie, video: Video)

  trait MovieDaoComponent {

    trait MovieDao {
      def getMovie(id: String): Movie
    }

  }

  trait FavoritesServiceComponent {

    trait FavoritesService {
      def getFavoriteVideos(id: String): Vector[Video]
    }

  }

  trait MovieDaoComponentImpl extends MovieDaoComponent {

    class MovieDaoImpl extends MovieDao {
      def getMovie(id: String): Movie = new Movie("42", "A Movie")
    }

  }

  trait FavoritesServiceComponentImpl extends FavoritesServiceComponent {

    class FavoritesServiceImpl extends FavoritesService {
      def getFavoriteVideos(id: String): Vector[Video] = Vector(new Video("100"), new Video("200"))
    }

  }

  trait MovieServiceComponentImpl {
    this: MovieDaoComponent with FavoritesServiceComponent =>

    val favoritesService: FavoritesService
    val movieDao: MovieDao

    class MovieServiceImpl {
      def getFavoriteDecoratedMovies(userId: String): Vector[DecoratedMovie] =
        for (
          favoriteVideo <- favoritesService.getFavoriteVideos(userId);
          movie = movieDao.getMovie(favoriteVideo.movieId)
        ) yield DecoratedMovie(movie, favoriteVideo)
    }

  }

  object ComponentRegistry extends MovieServiceComponentImpl
    with FavoritesServiceComponentImpl with MovieDaoComponentImpl {
    val favoritesService = new FavoritesServiceImpl
    val movieDao = new MovieDaoImpl

    val movieService = new MovieServiceImpl
  }

  val concreteMovieService = ComponentRegistry.movieService
  println(concreteMovieService.getFavoriteDecoratedMovies("1"))

  // test stub

  trait MovieDaoComponentTestImpl extends MovieDaoComponent {

    class MovieDaoTestImpl extends MovieDao {
      def getMovie(id: String): Movie = new Movie("43", "A Test Movie")
    }

  }

  trait FavoritesServiceComponentTestImpl extends FavoritesServiceComponent {

    class FavoritesServiceTestImpl extends FavoritesService {
      def getFavoriteVideos(id: String): Vector[Video] = Vector(new Video("2"))
    }

  }

  object TestComponentRegistery extends MovieServiceComponentImpl
    with FavoritesServiceComponentTestImpl with MovieDaoComponentTestImpl {
    val favoritesService = new FavoritesServiceTestImpl
    val movieDao = new MovieDaoTestImpl

    val movieService = new MovieServiceImpl
  }

}
