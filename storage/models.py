from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, Float, func

class Base(DeclarativeBase):
    pass

class CheckIn(Base):
    __tablename__ = "checkins"
    id = mapped_column(Integer, primary_key=True)
    library_id = mapped_column(String(250), nullable=False)
    library_name = mapped_column(String(250), nullable=False)
    entrance_id = mapped_column(String(250), nullable=False)
    entry_count = mapped_column(Integer, nullable=False)
    exit_count = mapped_column(Integer, nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    interval_start = mapped_column(DateTime, nullable=False)
    interval_end = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())
    trace_id = mapped_column(String(36), nullable=False, index=True)

    def to_dict(self):
        return {
            "library_id": self.library_id,
            "library_name": self.library_name,
            "entrance_id": self.entrance_id,
            "entry_count": self.entry_count,
            "exit_count": self.exit_count,
            "batch_timestamp": self.batch_timestamp.isoformat() + "Z",
            "interval_start": self.interval_start.isoformat() + "Z",
            "interval_end": self.interval_end.isoformat() + "Z",
            "trace_id": self.trace_id,
        }

class Borrowing(Base):
    __tablename__ = "borrowings"
    id = mapped_column(Integer, primary_key=True)
    library_id = mapped_column(String(250), nullable=False)
    library_name = mapped_column(String(250), nullable=False)
    book_id = mapped_column(String(250), nullable=False)
    genre = mapped_column(String(250), nullable=False)
    department = mapped_column(String(250), nullable=False)
    borrowed_at = mapped_column(DateTime, nullable=False)
    returned_at = mapped_column(DateTime)
    borrow_duration_days = mapped_column(Integer, nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())
    trace_id = mapped_column(String(36), nullable=False, index=True)

    def to_dict(self):
        return {
            "library_id": self.library_id,
            "library_name": self.library_name,
            "book_id": self.book_id,
            "genre": self.genre,
            "department": self.department,
            "borrowed_at": self.borrowed_at.isoformat() + "Z",
            "returned_at": self.returned_at.isoformat() + "Z" if self.returned_at else None,
            "borrow_duration_days": self.borrow_duration_days,
            "batch_timestamp": self.batch_timestamp.isoformat() + "Z",
            "trace_id": self.trace_id,
        }
