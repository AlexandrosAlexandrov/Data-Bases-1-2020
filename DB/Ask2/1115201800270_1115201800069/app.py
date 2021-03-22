import settings
import sys,os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db

def connection():
    con = db.connect(settings.mysql_host, settings.mysql_user, settings.mysql_passwd, settings.mysql_schema)
    return con

def extract_ngrams(text, number):
    # Convert the tuple to a string.
    text = str(text)

    # Split the string by the spaces and calculate the word count.
    words = text.split()
    wordCount = len(words)

    # Calculate the element count of the result based on the number of words per element.
    resultCount = wordCount - (number - 1)
    result = [""] * resultCount

    # Iterate through the words and add them to the result.
    for i in range(resultCount):
        # The first word is always in the result except the number of words per element.
        result[i] = words[i]

        # If we want two or more words in an element, add the second one.
        if number >= 2:
            result[i] += " " + words[i + 1]

        # If we want three words in an element, add the third one.
        if number == 3:
            result[i] += " " + words[i + 2]

    # Return the result.
    return result

def classify_review(review_id):
    # Connect to the database and get the cursor.
    con = connection()
    cursor = con.cursor()

    # Get all the positive terms and store them.
    positiveTerms = ""
    command = "SELECT * FROM posterms"

    try:
        cursor.execute(command)
        positiveTerms = cursor.fetchall()
    except:
        # If we can't get all the positive terms, print an error message and exit the function.
        print ("Could not query for all positive terms!")
        con.close()
        return

    # Get all the negative terms and store them.
    negativeTerms = ""
    command = "SELECT * FROM negterms"

    try:
        cursor.execute(command)
        negativeTerms = cursor.fetchall()
    except:
        # If we can't get all the negative terms, print an error message and exit the function.
        print ("Could not query for all negative terms!")
        con.close()
        return

    # Get the name of the business and the review text of the selected review.
    reviewInfo = ""
    command = "SELECT b.name, r.text FROM reviews r, business b WHERE r.review_id = '%s' AND b.business_id = r.business_id" % review_id

    try:
        cursor.execute(command)
        reviewInfo = cursor.fetchone()
    except:
        # If we can't get name of the business and the review text of the selected review, print an error message and exit the function.
        print ("Could not query for the business name and review text!")
        con.close()
        return

    # Close the database.
    con.close()

    # Covert from the tuple to variables for ease of use.
    businessName = reviewInfo[0]
    reviewText = reviewInfo[1]

    # Get all the ngrams of the review text.
    ngrams1 = extract_ngrams(reviewInfo, 1)
    ngrams2 = extract_ngrams(reviewInfo, 2)
    ngrams3 = extract_ngrams(reviewInfo, 3)

    # Calculate the positive and negative term count.
    positiveTermCount = 0
    negativeTermCount = 0

    # Allocating an array that holds whether an index of the words has already been found.
    foundWords = [False] * (len(("".join(reviewText)).split()) + 4)

    # Look for postive and negative terms in the ngrams with 3 words per element.
    index = 0
    for ngram3 in ngrams3:
        # For the ngrams with 3 words, we don't need to check for any words found, since we check them first.

        # Check for positive terms.
        for positiveTerm in positiveTerms:
            positiveTerm = positiveTerm[0] # Convert from tuple to string.

            # If a positive term was found, increment the positive term counter and ignore the words included in the ngram that could form a ngram with 2 words.
            if positiveTerm == ngram3:
                positiveTermCount += 3
                foundWords[index + 0] = True
                foundWords[index + 1] = True
                break

        # Check for negative terms.
        for negativeTerm in negativeTerms:
            negativeTerm = negativeTerm[0] # Convert from tuple to string.

            # If a positive term was found, increment the negative term counter and ignore the words included in the ngram that could form a ngram with 2 words.
            if negativeTerm == ngram3:
                negativeTermCount += 3
                foundWords[index + 0] = True
                foundWords[index + 1] = True
                break

        index += 1

   # Look for postive and negative terms in the ngrams with 2 words per element.
    index = 0
    for ngram2 in ngrams2:
        # If the index of the element in the ngram was found in the ngrams with 3 words per element, ignore it.
        if foundWords[index] == True:
            continue

        # Check for positive terms.
        for positiveTerm in positiveTerms:
            positiveTerm = positiveTerm[0] # Convert from tuple to string.

            # If a positive term was found, increment the positive term counter and ignore the words included in the ngram that could form a ngram with 1 word.
            if positiveTerm == ngram2:
                positiveTermCount += 2
                foundWords[index + 0] = True
                foundWords[index + 1] = True
                foundWords[index + 2] = True
                break

        # Check for negative terms.
        for negativeTerm in negativeTerms:
            negativeTerm = negativeTerm[0] # Convert from tuple to string.

            # If a negative term was found, increment the negative term counter and ignore the words included in the ngram that could form a ngram with 1 word.
            if negativeTerm == ngram2:
                negativeTermCount += 2
                foundWords[index + 0] = True
                foundWords[index + 1] = True
                foundWords[index + 2] = True
                break

        index += 1

   # Look for postive and negative terms in the ngrams with 1 word per element.
    index = 0
    for ngram1 in ngrams1:
        # If the index of the element in the ngram was found in the ngrams with 3 words or 2 words per element, ignore it.
        if foundWords[index] == True:
            continue

        # Check for positive terms.
        for positiveTerm in positiveTerms:
            positiveTerm = positiveTerm[0] # Convert from tuple to string.

            # If a positive term was found, increment the positive term counter. No need to ignore words here since this is the last check we do.
            if positiveTerm == ngram1:
                positiveTermCount += 1
                break

        # Check for negative terms.
        for negativeTerm in negativeTerms:
            negativeTerm = negativeTerm[0] # Convert from tuple to string.

            # If a negative term was found, increment the negative term counter. No need to ignore words here since this is the last check we do.
            if negativeTerm == ngram1:
                negativeTermCount += 1
                break

        index += 1

    # Calculate the positive/negative value based on the positive and negative term count in the review text.
    positiveNegative = positiveTermCount - negativeTermCount

    # Clamp the positive/negative value between 0 and 1.
    positiveNegative = max(0, positiveNegative)
    positiveNegative = min(1, positiveNegative)

    # Return the result.
    return [("business_name", "result"), (str(businessName), str(positiveNegative))]

def updatezipcode(business_id, zipcode):
    # Connect to the database and get the cursor.
    con = connection()
    cursor = con.cursor()

    # Update the zip code for the specified business.
    command = "UPDATE business SET zip_code = '%s' WHERE business_id = '%s'" % (zipcode, business_id)

    try:
        cursor.execute(command)
        result = "OK"
    except:
        # In case of an error, rollback the database and return error as a result.
        result = "Error"
        con.rollback()

    # Close the database.
    con.close()

    # Return the result.
    return [("result", result)]

def selectTopNbusinesses(category_id, n):
    # Make sure n is an integer.
    n = int(n)

    # Connect to the database and get the cursor.
    con = connection()
    cursor = con.cursor()

    # Get the all the busnesses in the specified category.
    businessesInCategory = ""
    businessCountInCategory = 0
    command = "SELECT bc.business_id FROM business_category bc WHERE bc.category_id = '%s'" % category_id

    try:
        cursor.execute(command)
        businessCountInCategory = cursor.rowcount
        businessesInCategory = cursor.fetchall()
    except:
        # If we can't get the businesses of the specified category, print an error message and exit the function.
        print ("Could not query for all businesses in the specified category!")
        con.close()
        return

    # Make an array of tuples, that holds the business ID and the count of positive reviews.
    sortedBusinesses = [("", "")] * businessCountInCategory
    index = 0

    for business in businessesInCategory:
        business = business[0] # Convert from tuple to string.

        # Get the positive review count of the business.
        positiveReviewCount = ""
        command = "SELECT COUNT(*) FROM reviews r, reviews_pos_neg rpn WHERE r.business_id = '%s' AND rpn.review_id = r.review_id AND rpn.positive = '1'" % business

        try:
            cursor.execute(command)
            positiveReviewCount = cursor.fetchone()
        except:
            # If we can't get the business positive review count, print an error message and exit the function.
            print ("Could not query for the positive review count of the business ID: \"%s\"!") % business
            con.close()
            return

        # Add the business ID and the review count to the array.
        sortedBusinesses[index] = (business, positiveReviewCount)
        index += 1

    # Close the database.
    con.close()

    # Define a new function used for sorting the business array based of the review count.
    def get_review_count(element):
        return element[1]

    # Sort the array based on the review count and in the reverse order.
    sortedBusinesses.sort(key=get_review_count, reverse=True)

    # Make an array for the final result of n elements, plus one for the header.
    result = [("", "")] * (n + 1)
    result[0] = ("business_id", "numberOfreviews")

    # Go through the first n businesses and store them in the final result array.
    for i in range(1, n + 1):
        result[i] = sortedBusinesses[i - 1]

    # Return the result.
    return result

# Utility function finding the influenced friends of a user recursively.
def traceUserFriendInfuence(user_id, user_review_date, business_id, depth):
    # Connect to the database and get the cursor.
    con = connection()
    cursor = con.cursor()

    # Get the reviews of the selected user's friend's reviews, that are about the same business and ordered by date.
    friendReviews = ""
    friendReviewCount = 0
    command = "SELECT DISTINCT r.user_id, r.date FROM reviews r WHERE r.user_id IN (SELECT f.friend_id FROM friends f WHERE f.user_id = '%s') AND r.business_id = '%s' ORDER BY r.date" % (user_id, business_id)

    try:
        cursor.execute(command)
        friendReviewCount = cursor.rowcount
        friendReviews = cursor.fetchall()
    except:
        print ("Fail")

    # Close the database.
    con.close()

    # Find the selected user's friends that he has influenced.
    influencedUsers = [""] * (depth * friendReviewCount) # This is dangerous and not the correct way, but allocate a generous amount. No time :(
    influencedUserIndex = 0

    for friendReview in friendReviews:
        # Make sure that the users review date is before the friend's review date.
        if (friendReview[1] > user_review_date):
            found = False

            # Make sure that the user is not included twice.
            for influencedUser in influencedUsers:
                if (friendReview[0] == influencedUser):
                    found = True
                    break

            if (found == False):
                # Add the influenced user to the array.
                influencedUsers[influencedUserIndex] = friendReview[0]
                influencedUserIndex += 1

                # If the depth is still more than one, use recursion to find this friends' influenced friends.
                if (depth > 1):
                    friendInfluencedUsers = traceUserFriendInfuence(friendReview[0], friendReview[1], business_id, depth - 1)
                    friendInfluencedUserCount = friendInfluencedUsers[0]
                    friendInfluencedUsers = friendInfluencedUsers[1]

                    # Add the friends' influenced friends to this one's.
                    for i in range(friendInfluencedUserCount):
                        influencedUsers[influencedUserIndex] = friendInfluencedUsers[0]
                        influencedUserIndex += 1

    influencedUserCount = influencedUserIndex

    # Return the result.
    return (influencedUserIndex, influencedUsers)

def traceUserInfuence(user_id, depth):
    # If depth is not set, print an error and exit the function.
    if (depth == None):
        print ("No depth value set!")
        return

    # Make sure that the depth is an integer.
    depth = int(depth)

    # Connect to the database and get the cursor.
    con = connection()
    cursor = con.cursor()

    # Get the reviews of the selected user.
    userReviews = ""
    userReviewCount = 0
    command = "SELECT r.date, r.business_id FROM reviews r WHERE r.user_id = '%s' ORDER BY r.business_id, r.date" % user_id

    try:
        cursor.execute(command)
        userReviewCount = cursor.rowcount
        userReviews = cursor.fetchall()
    except:
        print ("Fail")

    # Keep the oldest reviews for each business that the selected user has submited.
    orderedUserReviews = [("", "")] * userReviewCount
    orderedUserReviewIndex = 0

    for userReview in userReviews:
        found = False
        for i in range(orderedUserReviewIndex):
            if (userReview[1] == orderedUserReviews[i][1]):
                found = True
                break

        if (found == False):
            orderedUserReviews[orderedUserReviewIndex] = userReview
            orderedUserReviewIndex += 1

    orderedUserReviewCount = orderedUserReviewIndex

    # Get the reviews of the selected user's friend's reviews, that are about the same business and ordered first by business ID and then by date.
    friendReviews = ""
    friendReviewCount = 0
    command = "SELECT DISTINCT r.user_id, r.date, r.business_id FROM reviews r WHERE r.user_id IN (SELECT f.friend_id FROM friends f WHERE f.user_id = '%s') AND r.business_id IN (SELECT r.business_id FROM reviews r WHERE r.user_id = '%s') ORDER BY r.business_id, r.date" % (user_id, user_id)

    try:
        cursor.execute(command)
        friendReviewCount = cursor.rowcount
        friendReviews = cursor.fetchall()
    except:
        print ("Fail")

    # Close the database.
    con.close()

    # Find the selected user's friends that he has influenced.
    influencedUsers = [""] * (depth * friendReviewCount) # Not the correct way, but allocate a generous amount.
    influencedUserIndex = 0

    for friendReview in friendReviews:
        for i in range(orderedUserReviewCount):
            # Make sure that the users review date is before the friend's review date.
            if (friendReview[2] == orderedUserReviews[i][1] and friendReview[1] > orderedUserReviews[i][0]):
                found = False

                # Make sure that the user is not included twice.
                for influencedUser in influencedUsers:
                    if (friendReview[0] == influencedUser):
                        found = True
                        break

                if (found == False):
                    # Add the influenced user to the array.
                    influencedUsers[influencedUserIndex] = friendReview[0]
                    influencedUserIndex += 1

                    # If the depth is more than one, use recursion to find this friends' influenced friends.
                    if (depth > 1):
                        friendInfluencedUsers = traceUserFriendInfuence(friendReview[0], friendReview[1], friendReview[2], depth - 1)
                        friendInfluencedUserCount = friendInfluencedUsers[0]
                        friendInfluencedUsers = friendInfluencedUsers[1]

                        # Add the friends' influenced friends to this one's.
                        for i in range(friendInfluencedUserCount):
                            influencedUsers[influencedUserIndex] = friendInfluencedUsers[0]
                            influencedUserIndex += 1

    # Make an array for the final result of influencedUserCount elements, plus one for the header.
    influencedUserCount = influencedUserIndex
    result = [("", )] * (influencedUserCount + 1)
    result[0] = ("user_id", )

    # Go through the influencedUsers and store them in the final result array.
    influencedUserIndex = 0
    for i in range(1, influencedUserCount + 1):
        result[i] = (influencedUsers[i - 1], )

    # Return the result.
    return result
