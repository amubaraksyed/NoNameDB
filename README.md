# NoNameDB - L-Store Implementation
A thread-safe implementation of the L-Store database system with ACID transaction support.

## Team
- __Aadhil Mubarak Syed__
- __Mason McLuckie__
- __Nikolaj Pedersen__

## Features

### Core Database Components
- ✅ Thread-safe bufferpool with LRU replacement
- ✅ ACID transaction support
- ✅ Multi-version concurrency control
- ✅ B-Tree based indexing
- ✅ Two-phase locking with deadlock prevention
- ✅ Crash recovery logging
- ✅ Tail-record management with merge support

### Implementation Details

#### Storage Management
- Page-based storage with configurable page size
- Efficient tail record handling
- Automatic merge operations for tail records
- Persistent storage with crash recovery

#### Concurrency Control
- Two-phase locking (2PL) with NO-WAIT policy
- Shared (read) and exclusive (write) locks
- Transaction rollback support
- Deadlock prevention through timeout

#### Query Processing
- B-Tree indexing for efficient lookups
- Support for range queries
- Version-aware query execution
- Tail record chain traversal

#### Buffer Management
- LRU page replacement policy
- Dirty page tracking
- Pin count management
- Thread-safe page access

#### Recovery System
- Write-ahead logging
- Transaction and recovery point logging
- Crash recovery support
- Version chain maintenance

### Milestones
|Milestone 1: Core Components | Milestone 2: Storage & Recovery | Milestone 3: Concurrency |
|----------------------------|--------------------------------|------------------------|
| ✅ Query Processing         | ✅ Bufferpool Implementation    | ✅ Transaction Support  |
| ✅ Record Management        | ✅ Persistent Storage          | ✅ Locking System       |
| ✅ Table Operations         | ✅ Merge Operations            | ✅ Multi-threading      |
| ✅ Index Structure          | ✅ Recovery Logging            | ✅ Version Management   |

## Configuration
Key configuration parameters can be found in `config.py`:
- Page size: 4096 bytes
- Record size: 8 bytes (64-bit integers)
- Bufferpool size: 1000 pages
- Merge trigger: 2000 updates

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/NoNameDB.git
   cd NoNameDB
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

```bash
python __main__.py
```

To run existing tests:

```bash
chmod +x m1_tester.sh m2_tester.sh m3_tester.sh
./m1_tester.sh
./m2_tester.sh
./m3_tester.sh
```

