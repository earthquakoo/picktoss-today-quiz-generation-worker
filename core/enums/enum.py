from enum import Enum


class SubscriptionPlanType(Enum):
    FREE = "FREE"
    PRO = "PRO"


class QuizQuestionNum(Enum):
    FREE_PLAN_QUIZ_QUESTION_NUM = 5
    PRO_PLAN_QUIZ_QUESTION_NUM = 10


class DocumentStatus(Enum):
    UNPROCESSED = "UNPROCESSED"
    PROCESSED = "PROCESSED"
    COMPLETELY_FAILED = "COMPLETELY_FAILED"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    
class QuizType(Enum):
    MIX_UP = "MIX_UP"
    MULTIPLE_CHOICE = "MULTIPLE_CHOICE"