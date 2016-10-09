package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "tajik.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                       this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
        /*
        this.longestFirstNames.add("JohnJacobJingleheimerSchmidt");
        this.shortestFirstNames.add("Al");
        this.shortestFirstNames.add("Jo");
        this.shortestFirstNames.add("Bo");
        this.mostCommonFirstNames.add("John");
        this.mostCommonFirstNames.add("Jane");
        this.mostCommonFirstNamesCount = 10;
        */

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each user, count up the instances of each first name
            // and group count by first name in descending order
            ResultSet rst = stmt.executeQuery("select count(*) as numtimes, first_name from " +
                    userTableName +
                    " where first_name is not null group by first_name order by numtimes desc");

            while(rst.next()) {
                int count = rst.getInt(1);
                String first_name = rst.getNString(2);
                if(rst.getRow() == 1) {
                    this.mostCommonFirstNames.add(first_name);
                    this.mostCommonFirstNamesCount = count;
                }
                // In case of ties
                // If count in current, non-first row is equal to the count in the first now
                if(rst.getRow() != 1 && this.mostCommonFirstNamesCount == count) {
                    this.mostCommonFirstNames.add(first_name);
                }
            }

            // For each user, get the length of the first name and the first name
            // group by first name and order in desc order by length of first name
            rst = stmt.executeQuery("select length(first_name) as namelength, first_name from " +
                    userTableName + 
                    " where first_name is not null group by first_name order by namelength desc");
            //int num_rows_in_rst = 0;
             while(rst.next()) {
                int len = rst.getInt(1);
                String first_name = rst.getNString(2);
                // Since first row is going to be first name that is longest, add to container
                if(rst.getRow() == 1) {
                    this.longestFirstNames.add(first_name);
                }
                // In case of ties
                // If there are rows after the first that have the same length
                if(rst.getRow() != 1 && (this.longestFirstNames.first()).length() == len) {
                    this.longestFirstNames.add(first_name);
                }
                //num_rows_in_rst += 1;
            }

            // For each user, get the length of the first name and the first name
            // group by first name and order in asc order by length of first name
            rst = stmt.executeQuery("select length(first_name) as namelength, first_name from " +
                    userTableName + 
                    " where first_name is not null group by first_name order by namelength asc");
             while(rst.next()) {
                int len = rst.getInt(1);
                String first_name = rst.getNString(2);
                // Since first row is going to be first name that is longest, add to container
                if(rst.getRow() == 1) {
                    this.shortestFirstNames.add(first_name);
                }
                // In case of ties
                // If there are rows after the first that have the same length
                if(rst.getRow() != 1 && (this.shortestFirstNames.first()).length() == len) {
                    this.shortestFirstNames.add(first_name);
                }
            }


            // NOT WORKING
            /*
            num_rows_in_rst -= 1;

            // Read ResultSet backwards
            while(rst.previous()) {
                int len = rst.getInt(1);
                String first_name = rst.getNString(2);
                if(rst.getRow() == num_rows_in_rst) {
                    this.shortestFirstNames.add(first_name);
                }
                // In case of ties
                // If count in current, non-first row is equal to the count in the first now
                if(rst.getRow() != num_rows_in_rst && (this.shortestFirstNames.first()).length() == len) {
                    this.shortestFirstNames.add(first_name);
                }
            }
            */

            // Close statement and result set
            rst.close();
            stmt.close();
        
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }

    }

    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
        //this.lonelyUsers.add(new UserInfo(10L, "Billy", "SmellsFunny"));
        //this.lonelyUsers.add(new UserInfo(11L, "Jenny", "BadBreath"));
    
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

                    // For each user, count up the instances of each first name
                    // and group count by first name in descending order
                    ResultSet rst = stmt.executeQuery("select user_id from " +
                        userTableName +
                        " where not exists (select user1_id, user2_id from " +
                            friendsTableName +
                            " where user_id = user1_id OR user_id = user2_id)");

/*
                    while(rst.next()) {
                        int user_id = rst.getInt(1);
                        // Inputting into lonelyUsers, 10L, 11L?
                        System.out.print(user_id + "\n");
                    }
*/
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }

    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
        this.liveAwayFromHome.add(new UserInfo(11L, "Heather", "Movalot"));

        try (Statement stmt =
                 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                         ResultSet.CONCUR_READ_ONLY)) {

                // For each user, count up the instances of each first name
                // and group count by first name in descending order
                ResultSet rst = stmt.executeQuery("select U.user_id from " +
                    userTableName + " U, " + currentCityTableName + " CC, " + hometownCityTableName + " HC " +
                    " where U.user_id = CC.user_id AND U.user_id = HC.user_id AND CC.current_city_id != HC.hometown_city_id");

/*
                int num_rows = 0;
                while(rst.next()) {
                    num_rows++;
                    int user_id = rst.getInt(1);
                    // Inputting into lonelyUsers, 10L, 11L?
                    System.out.print(user_id + "\n");
                }
*/

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }

    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {
        String photoId = "1234567";
        String albumId = "123456789";
        String albumName = "album1";
        String photoCaption = "caption1";
        String photoLink = "http://google.com";
        PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
        TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
        tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
        tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
        this.photosWithMostTags.add(tp);


        try (Statement stmt =
                 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                         ResultSet.CONCUR_READ_ONLY)) {

                stmt.executeUpdate("CREATE VIEW photo_num_subjects (num_subjects, photo_id) as (select count(*) as numsubjects, tag_photo_id from " + 
                    tagTableName + 
                        " where tag_photo_id is not null group by tag_photo_id order by numsubjects desc)");




/*
                ResultSet rst = stmt.executeQuery("select U.user_id from " +
                    userTableName + " U, " + currentCityTableName + " CC, " + hometownCityTableName + " HC " +
                    " where U.user_id = CC.user_id AND U.user_id = HC.user_id AND CC.current_city_id != HC.hometown_city_id");
*/

                stmt.executeUpdate("DROP VIEW photo_num_subjects");

//CREATE VIEW photo_num_subjects (num_subjects, photo_id) AS SELECT COUNT(*) AS numsubjects, tag_photo_id FROM tajik.PUBLIC_TAGS WHERE tag_photo_id IS NOT NULL GROUP BY tag_photo_id;                
//CREATE VIEW most_tagged_photos (num_subjects, photo_id) AS SELECT * FROM photo_num_subjects WHERE num_subjects = (SELECT max(num_subjects) FROM photo_num_subjects) ORDER BY num_subjects asc;
/*
                int num_rows = 0;
                while(rst.next()) {
                    num_rows++;
                    int user_id = rst.getInt(1);
                    String photo_id = rst.getNString(2);
                    // Inputting into lonelyUsers, 10L, 11L?
                    System.out.print(user_id + " " + photo_id + "\n");
                }
*/

        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }

        // select photo_id, album_id, album_name, photo_caption, photo_link
        // from photos p, albums a
        // 




    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) One of the users is female, and the other is male
    // (2) Their age difference is within "yearDiff"
    // (3) They are not friends with one another
    // (4) They should be tagged together in at least one photo
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user_id for the female
    // (iii) If there are still ties, choose the pair with the smaller user_id for the male
    //
    public void matchMaker(int n, int yearDiff) {
        Long girlUserId = 123L;
        String girlFirstName = "girlFirstName";
        String girlLastName = "girlLastName";
        int girlYear = 1988;
        Long boyUserId = 456L;
        String boyFirstName = "boyFirstName";
        String boyLastName = "boyLastName";
        int boyYear = 1986;
        MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName,
                girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
        String sharedPhotoId = "12345678";
        String sharedPhotoAlbumId = "123456789";
        String sharedPhotoAlbumName = "albumName";
        String sharedPhotoCaption = "caption";
        String sharedPhotoLink = "link";
        mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
        this.bestMatches.add(mp);
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //
    @Override
    public void suggestFriendsByMutualFriends(int n) {
        Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        UsersPair p = new UsersPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

        p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
        p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
        p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
        this.suggestedUsersPairs.add(p);
    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
        this.eventCount = 12;
        this.popularStateNames.add("Michigan");
        this.popularStateNames.add("California");
    }

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
        this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
        this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
    }

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {
        Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
        this.siblings.add(s);
    }

}
