from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, ForeignKey, Text, BigInteger
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base


class Coach(Base):
    __tablename__ = "coaches"
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    first_name = Column(String(200))
    username = Column(String(200))
    phone = Column(String(50))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    students = relationship("Student", back_populates="coach", cascade="all, delete-orphan")
    lessons = relationship("Lesson", back_populates="coach", cascade="all, delete-orphan")
    payments = relationship("Payment", back_populates="coach", cascade="all, delete-orphan")
    notifications = relationship("Notification", back_populates="coach", cascade="all, delete-orphan")
    locations = relationship("Location", back_populates="coach", cascade="all, delete-orphan")


class Location(Base):
    """Training locations (halls)."""
    __tablename__ = "locations"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    name = Column(String(200), nullable=False)
    address = Column(String(500))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="locations")


class StudentSchedule(Base):
    """Student can have multiple locations with different schedules."""
    __tablename__ = "student_schedules"
    
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)  # Allow null until location is selected
    days = Column(String(100), default="1,3")  # Days of week (0=Mon, 6=Sun)
    times = Column(String(500), default='{"1": "18:00", "3": "18:00"}')  # JSON: {"day": "time"}
    duration = Column(Integer, default=90)  # Minutes
    is_primary = Column(Boolean, default=True)  # Primary or additional location
    created_at = Column(DateTime, default=datetime.utcnow)
    
    student = relationship("Student", back_populates="schedules")
    location = relationship("Location")
    
    def get_time_for_day(self, day_of_week):
        """Get lesson time for specific day at this location."""
        try:
            import json
            if not self.times:
                return '18:00'
            times = json.loads(self.times)
            day_str = str(day_of_week)
            if day_str in times:
                return times[day_str]
            if times:
                return list(times.values())[0]
            return '18:00'
        except Exception:
            return '18:00'
    
    def has_lesson_on_day(self, day_of_week):
        """Check if student has lesson at this location on given day."""
        if not self.days:
            return False
        # Strip spaces to handle "5, 6" format
        day_list = [d.strip() for d in self.days.split(",")]
        result = str(day_of_week) in day_list
        # Debug logging
        import logging
        logging.getLogger(__name__).info(f"DEBUG has_lesson_on_day: days={self.days}, day_list={day_list}, checking={day_of_week}, result={result}")
        return result


class Student(Base):
    __tablename__ = "students"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    name = Column(String(200), nullable=False)
    nickname = Column(String(100))
    phone = Column(String(50))
    parent_phone = Column(String(50))
    age = Column(Integer)
    birthday = Column(Date, nullable=True)
    notes = Column(Text)
    
    # DEPRECATED: Kept for backward compatibility
    # Use student_schedules table instead for multiple locations
    location = Column(String(200), default="Зал Break Wave")
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    lesson_days = Column(String(100), default="1,3")
    lesson_times = Column(String(500), default='{"1": "18:00", "3": "18:00"}')
    lesson_duration = Column(Integer, default=90)
    
    lesson_price = Column(Integer, default=150)
    lessons_count = Column(Integer, default=8)
    lessons_remaining = Column(Integer, default=8)
    is_unlimited = Column(Boolean, default=False)  # Unlimited subscription (month-based)
    subscription_start = Column(Date, nullable=True)
    subscription_end = Column(Date, nullable=True)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="students")
    location_ref = relationship("Location")
    lessons = relationship("Lesson", back_populates="student", cascade="all, delete-orphan")
    attendance_records = relationship("Attendance", back_populates="student", cascade="all, delete-orphan")
    payments = relationship("Payment", back_populates="student", cascade="all, delete-orphan")
    schedules = relationship("StudentSchedule", back_populates="student", cascade="all, delete-orphan")
    
    def get_attendance_stats(self):
        """Calculate attendance statistics."""
        total = len(self.attendance_records)
        present = sum(1 for a in self.attendance_records if a.status == "present")
        absent = sum(1 for a in self.attendance_records if a.status == "absent")
        sick = sum(1 for a in self.attendance_records if a.status == "sick")
        return {"total": total, "present": present, "absent": absent, "sick": sick}
    
    def get_schedules_for_day(self, day_of_week):
        """Get all schedules (locations) for a specific day.
        
        Returns list of tuples: (schedule, location_name, time)
        Student may have multiple lessons on same day at different locations.
        """
        result = []
        
        # First check new schedules table
        if self.schedules:
            for schedule in self.schedules:
                if schedule.has_lesson_on_day(day_of_week):
                    time = schedule.get_time_for_day(day_of_week)
                    loc_name = schedule.location.name if schedule.location else "Зал"
                    result.append({
                        "schedule": schedule,
                        "location_id": schedule.location_id,
                        "location_name": loc_name,
                        "time": time,
                        "is_primary": schedule.is_primary
                    })
        
        # Fallback to legacy fields if no schedules defined
        if not result and self.lesson_days:
            days = [d.strip() for d in self.lesson_days.split(",")] if self.lesson_days else []
            if str(day_of_week) in days:
                time = self.get_lesson_time_for_day(day_of_week)
                loc_name = self.location or "Зал"
                result.append({
                    "schedule": None,
                    "location_id": self.location_id,
                    "location_name": loc_name,
                    "time": time,
                    "is_primary": True
                })
        
        return result
    
    def get_all_lesson_times_for_day(self, day_of_week):
        """Get all lesson times for a day (for students with multiple locations)."""
        schedules = self.get_schedules_for_day(day_of_week)
        return [s["time"] for s in schedules]
    
    def has_lesson_on_day(self, day_of_week):
        """Check if student has any lesson on given day."""
        return len(self.get_schedules_for_day(day_of_week)) > 0
    
    def get_lesson_time_for_day(self, day_of_week):
        """Get lesson time for specific day of week (legacy fallback)."""
        try:
            import json
            if not self.lesson_times:
                return '18:00'
            times = json.loads(self.lesson_times)
            day_str = str(day_of_week)
            if day_str in times:
                return times[day_str]
            if times:
                return list(times.values())[0]
            return '18:00'
        except Exception:
            return '18:00'
    
    def get_primary_schedule(self):
        """Get primary schedule (for backward compatibility)."""
        if self.schedules:
            primary = next((s for s in self.schedules if s.is_primary), None)
            if primary:
                return primary
            return self.schedules[0] if self.schedules else None
        return None


class Lesson(Base):
    __tablename__ = "lessons"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    date = Column(Date, nullable=False)
    time = Column(String(10))
    location = Column(String(200))
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    topic = Column(String(200))
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="lessons")
    student = relationship("Student", back_populates="lessons")
    location_ref = relationship("Location")
    attendance = relationship("Attendance", back_populates="lesson", uselist=False, cascade="all, delete-orphan")


class Attendance(Base):
    __tablename__ = "attendance"
    
    id = Column(Integer, primary_key=True)
    lesson_id = Column(Integer, ForeignKey("lessons.id"), nullable=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    status = Column(String(20), default="present")
    is_extra = Column(Boolean, default=False)
    attendance_date = Column(Date, nullable=False)
    attendance_time = Column(String(10))
    notes = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    lesson = relationship("Lesson", back_populates="attendance")
    student = relationship("Student", back_populates="attendance_records")
    location = relationship("Location")


class Payment(Base):
    __tablename__ = "payments"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    amount = Column(Integer, nullable=False)
    lessons_count = Column(Integer, default=8)
    status = Column(String(20), default="pending")  # paid, pending, overdue
    period_start = Column(Date)
    period_end = Column(Date)
    paid_at = Column(DateTime, nullable=True)
    notes = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="payments")
    student = relationship("Student", back_populates="payments")


class AdminUser(Base):
    __tablename__ = "admin_users"
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    first_name = Column(String(200))
    username = Column(String(200))
    created_at = Column(DateTime, default=datetime.utcnow)


class Notification(Base):
    __tablename__ = "notifications"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=True)
    type = Column(String(50), nullable=False)  # payment_due, subscription_ending, lesson_reminder, daily_digest
    message = Column(Text, nullable=False)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="notifications")


class DailyNotificationLog(Base):
    """Tracks which daily notifications have been sent to avoid duplicates."""
    __tablename__ = "daily_notification_logs"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    notification_type = Column(String(50), nullable=False)  # payment_due, low_lessons
    sent_at = Column(DateTime, default=datetime.utcnow)
    date = Column(Date, nullable=False)  # The date for which notification was sent
