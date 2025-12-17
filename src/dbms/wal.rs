use std::{cmp::Ordering, fs::File, io::{self, Read, Seek, Write}, marker::PhantomData, sync::{Arc, Mutex}};

pub trait WriteAheadLog {
    type Event;

    fn record(&self, event: Self::Event) -> io::Result<()>;
}

impl<WAL: WriteAheadLog> WriteAheadLog for Option<WAL> {
    type Event = WAL::Event;

    fn record(&self, event: Self::Event) -> io::Result<()> {
        if let Some(wal) = self {
            wal.record(event)
        } else {
            Ok(())
        }
    }
}

pub struct ConvertWAL<EventInto, WAL> {
    wal: WAL,
    _marker: PhantomData<EventInto>,
}

impl<EventInto, EventFrom, WAL> WriteAheadLog for ConvertWAL<EventInto, WAL>
where
    WAL: WriteAheadLog<Event = EventFrom>,
    EventInto: Into<EventFrom>,
{
    type Event = EventInto;

    fn record(&self, event: Self::Event) -> io::Result<()> {
        self.wal.record(event.into())
    }
}

impl<EventInto, WAL> ConvertWAL<EventInto, WAL> {
    pub fn new(wal: WAL) -> Self {
        Self {
            wal,
            _marker: PhantomData,
        }
    }
}

pub trait SerializableEvent: Sized {
    fn write(&self, writer: &mut impl io::Write) -> io::Result<()>;
    fn read(reader: &mut impl io::Read) -> io::Result<Self>;
}

struct FileWALInner {
    file: File,
    height: u64,
}

#[derive(Clone)]
pub struct FileWAL<Event> {
    inner: Arc<Mutex<FileWALInner>>,
    _marker: PhantomData<Event>,
}

impl<Event> FileWAL<Event> {
    pub fn load(mut file: File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        file.seek(io::SeekFrom::Start(0))?;
        let height = if len == 0 {
            file.write_all(&8u64.to_le_bytes())?;
            8
        } else {
            let mut buffer = [0u8; 8];
            file.read_exact(&mut buffer).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to read WAL height"))?;
            let height = u64::from_le_bytes(buffer);
            if height < 8 || height > len {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "WAL height is invalid"));
            }
            height
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(FileWALInner {
                file,
                height,
            })),
            _marker: PhantomData,
        })
    }

    pub fn sync(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let height = inner.height;
        inner.file.seek(io::SeekFrom::Start(0))?;
        inner.file.write_all(&height.to_le_bytes())?;
        inner.file.sync_all()
    }

    pub fn clear(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        inner.file.seek(io::SeekFrom::Start(0))?;
        inner.file.write_all(&8u64.to_le_bytes())?;
        inner.height = 8;
        Ok(())
    }
}

impl<Event> WriteAheadLog for FileWAL<Event>
where
    Event: SerializableEvent,
{
    type Event = Event;

    fn record(&self, event: Self::Event) -> io::Result<()> {
        let mut inner = self.inner.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let height = inner.height;
        inner.file.seek(io::SeekFrom::Start(height))?;
        event.write(&mut inner.file)?;
        inner.height = inner.file.stream_position()?;
        Ok(())
    }
}

pub trait WALReader {
    type Event;

    fn read_next(&mut self) -> io::Result<Option<Self::Event>>;
}

pub struct FileWALReader<Event> {
    height: Option<u64>,
    file: File,
    _marker: PhantomData<Event>,
}

impl<Event> FileWALReader<Event>
{
    pub fn new(mut file: File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(Self {
                height: None,
                file,
                _marker: PhantomData,
            })
        };

        file.seek(io::SeekFrom::Start(0))?;
        let mut buffer = [0u8; 8];
        file.read_exact(&mut buffer).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to read WAL height"))?;
        let height = u64::from_le_bytes(buffer);

        if height < 8 || height > len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "WAL height is invalid"));
        }

        Ok(Self {
            height: Some(height),
            file,
            _marker: PhantomData,
        })
    }

    pub fn into_file(self) -> File {
        self.file
    }
}

impl<Event> WALReader for FileWALReader<Event>
where
    Event: SerializableEvent,
{
    type Event = Event;

    fn read_next(&mut self) -> io::Result<Option<Event>> {
        let Some(height) = self.height else {
            return Ok(None);
        };
        match self.file.stream_position()?.cmp(&height) {
            Ordering::Equal => Ok(None),
            Ordering::Greater => {
                Err(io::Error::new(io::ErrorKind::InvalidData, "WAL reader position exceeded height"))
            },
            Ordering::Less => {
                let event = Event::read(&mut self.file)?;
                Ok(Some(event))
            },
        }
    }
}
