from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from core.database import get_db
from core.models.user import User
from core.schemas.user import UserCreate, User as UserSchema, Token
from utils.auth import verify_password, get_password_hash, create_access_token

class AuthController:
    @staticmethod
    def register(user: UserCreate, db: Session):
        # 检查用户名是否已存在
        db_user = db.query(User).filter(User.username == user.username).first()
        if db_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )
        # 检查邮箱是否已存在
        db_user = db.query(User).filter(User.email == user.email).first()
        if db_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        # 创建新用户
        hashed_password = get_password_hash(user.password)
        db_user = User(
            username=user.username,
            email=user.email,
            hashed_password=hashed_password
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return db_user

    @staticmethod
    def login(username: str, password: str, db: Session):
        # 查找用户
        user = db.query(User).filter(User.username == username).first()
        if not user or not verify_password(password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # 创建访问令牌
        access_token = create_access_token(data={"sub": user.username})
        return Token(access_token=access_token, token_type="bearer")