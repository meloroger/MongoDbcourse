package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;
import java.text.MessageFormat;
import java.util.Map;

import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

  private final MongoCollection<User> usersCollection;
  // User Management - do the necessary changes so that the sessions collection
  //returns a Session object
  private final MongoCollection<Session> sessionsCollection;

  private final Logger log;

  @Autowired
  public UserDao(
      MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
    super(mongoClient, databaseName);
    CodecRegistry pojoCodecRegistry =
        fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
    log = LoggerFactory.getLogger(this.getClass());
    //User Management - implement the necessary changes so that the sessions
    // collection returns a Session objects instead of Document objects.
    sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
  }

  /**
   * Inserts the `user` object in the `users` collection.
   *
   * @param user - User object to be added
   * @return True if successful, throw IncorrectDaoOperation otherwise
   */
  public boolean addUser(User user) {
    //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
    usersCollection.insertOne(user);
    return true;
    //TODO > Ticket: Handling Errors - make sure to only add new users
    // and not users that already exist.

  }

  /**
   * Creates session using userId and jwt token.
   *
   * @param userId - user string identifier
   * @param jwt - jwt string token
   * @return true if successful
   */
  public boolean createUserSession(String userId, String jwt) {

      Session newSession = new Session();
      newSession.setUserId(userId);
      newSession.setJwt(jwt);

      System.out.println(newSession.getUserId());

      //sessionsCollection.insertOne(newSession);
      //return true;

      Bson query = new Document("user_id", userId);
      UpdateOptions options = new UpdateOptions();
      options.upsert(true);
      sessionsCollection.updateOne(query, new Document("$set", newSession), options);
      return true;

    // User Management - implement the method that allows session information to be
    // stored in it's designated collection.
    //TODO > Ticket: Handling Errors - implement a safeguard against
    // creating a session with the same jwt token.
  }

  /**
   * Returns the User object matching the an email string value.
   *
   * @param email - email string to be matched.
   * @return User object or null.
   */
  public User getUser(String email) {
    User user = null;

    user = usersCollection.find(Filters.eq("email", email)).first();
    // User Management - implement the query that returns the first User object.
    return user;
  }

  /**
   * Given the userId, returns a Session object.
   *
   * @param userId - user string identifier.
   * @return Session object or null.
   */
  public Session getUserSession(String userId) {

    Session session = null;


    session = sessionsCollection.find(new Document("user_id", userId)).first();

    // User Management - implement the method that returns Sessions for a given
    // userId
    return session;
  }

  public boolean deleteUserSessions(String userId) {
    Session delete = getUserSession(userId);

    if(delete != null) {
      Bson doc = new Document("user_id", userId);
      sessionsCollection.deleteOne(doc);
      return true;
    }
    // User Management - implement the delete user sessions method
    return false;
  }

  /**
   * Removes the user document that match the provided email.
   *
   * @param email - of the user to be deleted.
   * @return true if user successfully removed
   */
  public boolean deleteUser(String email) {
    User user = getUser(email);
    if(user != null) {
      Bson query = new Document("email", email);
      usersCollection.deleteOne(query);
      deleteUserSessions(email);
      return true;
    }
    // remove user sessions
    // User Management - implement the delete user method
    //TODO > Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions.
    return false;
  }

  /**
   * Updates the preferences of an user identified by `email` parameter.
   *
   * @param email - user to be updated email
   * @param userPreferences - set of preferences that should be stored and replace the existing
   *     ones. Cannot be set to null value
   * @return User object that just been updated.
   */
  public boolean updateUserPreferences(String email,  Map<String, ?> userPreferences) throws IncorrectDaoOperation{
    User user = getUser(email);

    if(userPreferences == null) {
      throw new IncorrectDaoOperation("User Preferences can't be null");
    }else if(user != null) {
        Bson queryFilter = new Document("email", email);
        usersCollection.updateOne(queryFilter, set("preferences", userPreferences));
        return true;
     }

    //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
    // be updated.
    //TODO > Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions when updating an entry.
    return false;
  }
}
