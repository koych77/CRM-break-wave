from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, ForeignKey, Text, BigInteger, UniqueConstraint
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
    is_admin = Column(Boolean, default=False)
    is_manager = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    students = relationship("Student", back_populates="coach", cascade="all, delete-orphan")
    lessons = relationship("Lesson", back_populates="coach", cascade="all, delete-orphan")
    payments = relationship(
        "Payment",
        back_populates="coach",
        cascade="all, delete-orphan",
        foreign_keys="Payment.coach_id",
    )
    notifications = relationship("Notification", back_populates="coach", cascade="all, delete-orphan")
    locations = relationship("Location", back_populates="coach", cascade="all, delete-orphan")
    groups = relationship("TrainingGroup", back_populates="coach", cascade="all, delete-orphan")


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


class TrainingGroup(Base):
    """A mutable training group assigned to a trainer and, optionally, a hall."""
    __tablename__ = "training_groups"

    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    name = Column(String(200), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    coach = relationship("Coach", back_populates="groups")
    location = relationship("Location")


class ParentAccount(Base):
    """Telegram account of the only registering parent for one or more children."""
    __tablename__ = "parent_accounts"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    full_name = Column(String(200), nullable=False)
    phone = Column(String(50), nullable=False)
    training_reminders_enabled = Column(Boolean, default=True)
    bot_blocked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    students = relationship("Student", back_populates="parent")


class StudentSchedule(Base):
    """Student can have multiple locations with different schedules."""
    __tablename__ = "student_schedules"
    
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)  # Allow null until location is selected
    group_id = Column(Integer, ForeignKey("training_groups.id"), nullable=True)
    days = Column(String(100), default="1,3")  # Days of week (0=Mon, 6=Sun)
    times = Column(String(500), default='{"1": "18:00", "3": "18:00"}')  # JSON: {"day": "time"}
    duration = Column(Integer, default=90)  # Minutes
    is_primary = Column(Boolean, default=True)  # Primary or additional location
    created_at = Column(DateTime, default=datetime.utcnow)
    
    student = relationship("Student", back_populates="schedules")
    location = relationship("Location")
    group = relationship("TrainingGroup")
    
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
        return str(day_of_week) in day_list


class Student(Base):
    __tablename__ = "students"
    
    id = Column(Integer, primary_key=True)
    coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    parent_id = Column(Integer, ForeignKey("parent_accounts.id"), nullable=True)
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
    training_start_date = Column(Date, nullable=True)
    
    is_active = Column(Boolean, default=True)
    training_reminders_enabled = Column(Boolean, default=True)
    deleted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="students")
    parent = relationship("ParentAccount", back_populates="students")
    location_ref = relationship("Location")
    lessons = relationship("Lesson", back_populates="student", cascade="all, delete-orphan")
    attendance_records = relationship("Attendance", back_populates="student", cascade="all, delete-orphan")
    payments = relationship("Payment", back_populates="student", cascade="all, delete-orphan")
    schedules = relationship("StudentSchedule", back_populates="student", cascade="all, delete-orphan")

    @property
    def lesson_time(self):
        """Legacy compatibility accessor for code paths expecting a single lesson time."""
        primary = self.get_primary_schedule()
        if primary and primary.days:
            try:
                first_day = int(primary.days.split(",")[0].strip())
                return primary.get_time_for_day(first_day)
            except Exception:
                pass

        if self.lesson_days:
            try:
                first_day = int(self.lesson_days.split(",")[0].strip())
                return self.get_lesson_time_for_day(first_day)
            except Exception:
                pass

        return "18:00"
    
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
        
        # Fallback to legacy fields ONLY if no schedules table exists at all
        # Do NOT fallback if schedules exist but don't match this day
        if not self.schedules and self.lesson_days:
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
    source = Column(String(20), default="scheduled")  # scheduled, extra, makeup, coach_cancelled
    makeup_credit_id = Column(Integer, ForeignKey("makeup_credits.id"), nullable=True)
    deducted = Column(Boolean, default=False)
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
    tariff_code = Column(String(20), nullable=True)
    base_amount = Column(Integer, nullable=True)
    late_fee_amount = Column(Integer, default=0)
    due_date = Column(Date, nullable=True)
    payment_method = Column(String(20), nullable=True)  # online, cash
    receipt_file_id = Column(String(500), nullable=True)
    reported_at = Column(DateTime, nullable=True)
    confirmed_by_coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=True)
    cash_received_by_coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=True)
    rejection_reason = Column(String(500), nullable=True)
    period_start = Column(Date)
    period_end = Column(Date)
    is_unlimited = Column(Boolean, default=False)
    paid_at = Column(DateTime, nullable=True)
    notes = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    coach = relationship("Coach", back_populates="payments", foreign_keys=[coach_id])
    student = relationship("Student", back_populates="payments")


class RegistrationInvite(Base):
    """One-time, seven-day invitation created by an administrator."""
    __tablename__ = "registration_invites"

    id = Column(Integer, primary_key=True)
    token = Column(String(100), unique=True, nullable=False)
    preliminary_child_name = Column(String(200), nullable=False)
    created_by_coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="active")  # active, used, cancelled
    created_at = Column(DateTime, default=datetime.utcnow)


class RegistrationRequest(Base):
    """Parent-submitted child registration waiting for administrator approval."""
    __tablename__ = "registration_requests"

    id = Column(Integer, primary_key=True)
    invite_id = Column(Integer, ForeignKey("registration_invites.id"), unique=True, nullable=False)
    parent_id = Column(Integer, ForeignKey("parent_accounts.id"), nullable=False)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=True)
    child_name = Column(String(200), nullable=False)
    child_birthday = Column(Date, nullable=False)
    child_phone = Column(String(50), nullable=True)
    proposed_schedule = Column(Text, nullable=False)
    status = Column(String(20), default="pending")
    rejection_reason = Column(String(500), nullable=True)
    reviewed_by_coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ScheduleRequest(Base):
    """A parent request to change the agreed schedule."""
    __tablename__ = "schedule_requests"

    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    parent_id = Column(Integer, ForeignKey("parent_accounts.id"), nullable=False)
    proposed_schedule = Column(Text, nullable=False)
    status = Column(String(20), default="pending")
    rejection_reason = Column(String(500), nullable=True)
    reviewed_by_coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class MakeupCredit(Base):
    """Right to one makeup training after an absence or trainer cancellation."""
    __tablename__ = "makeup_credits"

    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    source_attendance_id = Column(Integer, ForeignKey("attendance.id"), nullable=True)
    source_date = Column(Date, nullable=False)
    source_type = Column(String(30), default="absence")
    expires_at = Column(Date, nullable=False)
    status = Column(String(20), default="available")
    requested_date = Column(Date, nullable=True)
    scheduled_date = Column(Date, nullable=True)
    scheduled_time = Column(String(10), nullable=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    group_id = Column(Integer, ForeignKey("training_groups.id"), nullable=True)
    approved_by_coach_id = Column(Integer, ForeignKey("coaches.id"), nullable=True)
    rejection_reason = Column(String(500), nullable=True)
    used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class TrainingResponse(Base):
    """Parent response to the day-before training reminder."""
    __tablename__ = "training_responses"
    __table_args__ = (
        UniqueConstraint(
            "student_id",
            "training_date",
            "training_time",
            "location_id",
            name="uq_training_response_slot",
        ),
    )

    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    training_date = Column(Date, nullable=False)
    training_time = Column(String(10), nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    response = Column(String(20), nullable=False)  # attending, absent
    responded_at = Column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    """Compact audit trail for sensitive CRM actions."""
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True)
    actor_telegram_id = Column(BigInteger, nullable=True)
    action = Column(String(100), nullable=False)
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(Integer, nullable=True)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


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
    recipient_telegram_id = Column(BigInteger, nullable=True)
    is_read = Column(Boolean, default=False)
    sent_at = Column(DateTime, nullable=True)
    delivery_error = Column(String(500), nullable=True)
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
