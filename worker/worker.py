import os
import logging
import random
import uuid
import pytz
from collections import defaultdict
from datetime import datetime

from core.database.database_manager import DatabaseManager
from core.email.email_manager import EmailManager
from constant.constant import FREE_PLAN_QUIZ_QUESTION_NUM, PRO_PLAN_QUIZ_QUESTION_NUM
from core.enums.enum import SubscriptionPlanType


logging.basicConfig(level=logging.INFO)


def handler(event, context):
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])
    email_manager = EmailManager(mailgun_api_key=os.environ["MAILGUN_API_KEY"], mailgun_domain=os.environ["MAILGUN_DOMAIN"])
    
    get_member_query = "SELECT * FROM member"
    members: list[dict] = db_manager.execute_query(get_member_query)
    for member in members:
        subscription_select_query = f"SELECT * FROM subscription WHERE member_id = {member['id']}"
        subscriptions = db_manager.execute_query(subscription_select_query)
        subscription = subscriptions[0]
        
        candidate_quiz_map: dict[int, list] = defaultdict(list)
        total_quiz_count = 0
        get_category_query = f"SELECT * FROM category WHERE member_id = {member['id']}"
        categories: list[dict] = db_manager.execute_query(get_category_query)
        for category in categories:
            get_document_query = f"SELECT * FROM document WHERE category_id = {category['id']}"
            documents: list[dict] = db_manager.execute_query(get_document_query)
            for document in documents:
                get_quiz_query = f"SELECT * FROM quiz WHERE document_id = {document['id']}"
                quizzes: list[dict] = db_manager.execute_query(get_quiz_query)
                for quiz in quizzes:
                    delivered_count = quiz["delivered_count"]
                    candidate_quiz_map[delivered_count].append(quiz)
                    total_quiz_count += 1
        
        if total_quiz_count <= 5:
            continue
        
        delivery_quizzes = []
        DELIVERY_QUESTIION_NUM = FREE_PLAN_QUIZ_QUESTION_NUM if subscription['plan_type'] == SubscriptionPlanType.FREE.value else PRO_PLAN_QUIZ_QUESTION_NUM
        
        current_count = 0
        sorted_keys = sorted(candidate_quiz_map.keys())
        full_flag = False
        
        logging.info(f"Subscription plan type: {subscription['plan_type']}")
        logging.info(f"Delivery question number: {DELIVERY_QUESTIION_NUM}")

        # Iterate prioritizing questions with less delivery count
        for delivered_count in sorted_keys:
            candidate_questions = candidate_quiz_map[delivered_count]
            random.shuffle(candidate_questions)
            logging.info(f"Delivering questions with delivered_count {delivered_count}")

            for candidate_question in candidate_questions:
                delivery_quizzes.append(candidate_question)
                current_count += 1
                logging.info(f"Current count: {current_count}")
                
                if current_count == DELIVERY_QUESTIION_NUM:
                    full_flag = True
                    logging.info("Reached delivery question limit")
                    break
            if full_flag:
                break
        
        quiz_set_id = uuid.uuid4().hex
        quiz_set_insert_query = "INSERT INTO quiz_set (id, solved, is_today_quiz_set, member_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        timestamp_now = datetime.now(pytz.timezone('Asia/Seoul'))
        db_manager.execute_query(quiz_set_insert_query, (quiz_set_id, False, True, member['id'], timestamp_now, timestamp_now))
        
        for delivery_quiz in delivery_quizzes:
            quiz_set_quiz_inset_query = "INSERT INTO quiz_set_quiz (quiz_id, quiz_set_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
            db_manager.execute_query(quiz_set_quiz_inset_query, (delivery_quiz['id'], quiz_set_id, timestamp_now, timestamp_now))
            
            quiz_delivered_count_update_query = f"UPDATE quiz SET delivered_count = delivered_count + 1 WHERE id = {delivery_quiz['id']}"
            db_manager.execute_query(quiz_delivered_count_update_query)
        
        if member['email']:        
            content = email_manager.read_and_format_html(
                replacements={"__QUESTION_LINK__": f"https://www.picktoss.com/random?question_set_id={quiz_set_id}"}
            )

            email_manager.send_email(recipient=member['email'], subject="🚀 오늘의 퀴즈가 도착했습니다!", content=content)
                
        db_manager.commit()

    return {"statusCode": 200, "message": "hi"}