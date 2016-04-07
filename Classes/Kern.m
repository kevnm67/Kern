
#import "Kern.h"
#import "NSURL+DoNotBackup.h"

NSString *const kKernDefaultStoreFileName = @"KernDataStore.sqlite";
NSString *const kKernDefaultBaseName      = @"Kern";
static NSPersistentStore *_persistentStore;
static NSManagedObjectModel *_managedObjectModel;
static NSManagedObjectContext *_privateQueueContext;
static NSManagedObjectContext *_mainQueueContext;

# pragma mark - Private Declarations
@interface Kern ()

+ (NSString *)baseName;
+ (NSURL *)   applicationDocumentsDirectory;
+ (void)      createApplicationSupportDirIfNeeded;
+ (void)      setupAutoMigratingCoreDataStack:(BOOL)shouldAddDoNotBackupAttribute;

+ (void)kern_didSaveContext:(NSNotification *)notification;
+ (NSUInteger)kern_countForFetchRequest:(NSFetchRequest *)fetchRequest;
+ (NSArray *)kern_executeFetchRequest:(NSFetchRequest *)fetchRequest;

@end

# pragma mark - Kern Implementation
@implementation Kern

#pragma mark - Accessors

// the shared model
+ (NSManagedObjectModel *)sharedModel {
    if (!_managedObjectModel) {
        [self setupInMemoryStoreCoreDataStack];  // if nothing was setup, we'll use an in memory store
    }
    
    return _managedObjectModel;
}

// the shared context
+ (NSManagedObjectContext *)sharedContext {
    if (!_mainQueueContext) {
        [self setupInMemoryStoreCoreDataStack];  // if nothing was setup, we'll use an in memory store
    }
    
    return _mainQueueContext;
}

#pragma mark - Path Helpers

+ (NSString *)baseName {
    NSString *defaultName = [[[NSBundle bundleForClass:[NSBundle mainBundle].class] infoDictionary] valueForKey:(id)kCFBundleNameKey];
    
    return (defaultName != nil) ? defaultName : kKernDefaultBaseName;
}

// Returns the URL to the application's Documents directory.
+ (NSURL *)applicationDocumentsDirectory {
    return [[[NSFileManager defaultManager] URLsForDirectory:NSDocumentDirectory inDomains:NSUserDomainMask] lastObject];
}

// Create the folder structure for the data store if it doesn't exist
+ (void)createApplicationSupportDirIfNeeded {
    return [self createApplicationSupportDirIfNeededForStore:[self baseName]];
}

// Create the folder structure for the data store if it doesn't exist
+ (void)createApplicationSupportDirIfNeededForStore:(NSString *)storeName {
    if ([[NSFileManager defaultManager] fileExistsAtPath:[[self urlForStoreName:storeName] absoluteString]]) {
        return;
    }
    
    [[NSFileManager defaultManager] createDirectoryAtURL:[[self urlForStoreName:storeName] URLByDeletingLastPathComponent]
                             withIntermediateDirectories:YES attributes:nil error:nil];
}

// Return the full path to the data store
+ (NSURL *)storeURL {
    return [self urlForStoreName:[self baseName]];
}

+ (NSURL *)urlForStoreName:(NSString *)storeName {
    return [[[self applicationDocumentsDirectory] URLByAppendingPathComponent:storeName] URLByAppendingPathExtension:@"sqlite"];
}

#pragma mark - Core Data Setup

+ (void)setupCoreDataStackWithAutoMigratingSqliteStoreNamed:(NSString *)storeName {
    // Set the default data store (ie pre-loaded SQL file) as initial store
    [self setInitialDataStoreNamed:storeName];
    [self setupAutoMigratingCoreDataStack:NO withStoreName:storeName];
}

+ (void)setupAutoMigratingCoreDataStack:(BOOL)shouldAddDoNotBackupAttribute {
    [self setupAutoMigratingCoreDataStack:shouldAddDoNotBackupAttribute withStoreName:nil];
}

+ (void)setupAutoMigratingCoreDataStack {
    [self setupAutoMigratingCoreDataStack:NO];
}

+ (void)setupAutoMigratingCoreDataStackWithDoNotBackupAttribute {
    [self setupAutoMigratingCoreDataStack:YES];
}

+ (void)setupAutoMigratingCoreDataStack:(BOOL)shouldAddDoNotBackupAttribute withStoreName:(NSString *)storeName {
    // setup our object model and persistent store
    _managedObjectModel = [NSManagedObjectModel mergedModelFromBundles:nil];
    
    NSPersistentStoreCoordinator *coordinator = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:_managedObjectModel];
    
    // create the folder if we need it
    [self createApplicationSupportDirIfNeededForStore:storeName];
    
    // If the name of the persistent store if provided, use it, otherwise use the name of the application for the store file name.
    NSURL *storeURL = storeName ? [self urlForStoreName:storeName] : [self storeURL];
    
    if (shouldAddDoNotBackupAttribute) {
        // add do not backup flag
        [storeURL addSkipBackupAttribute];
    }
    
    // attempt to create the store
    NSError *error = nil;
    _persistentStore = [coordinator addPersistentStoreWithType:NSSQLiteStoreType configuration:nil URL:storeURL options:[self defaultMigrationOptions] error:&error];
    
    if (!_persistentStore || error) {
        NSLog(@"Unable to create persistent store! %@, %@", error, error.userInfo);
    }
    
    [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(kern_didSaveContext:) name:NSManagedObjectContextDidSaveNotification object:nil];
    
    _privateQueueContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
    [_privateQueueContext setPersistentStoreCoordinator:coordinator];
    _privateQueueContext.undoManager = nil;
    
    _mainQueueContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSMainQueueConcurrencyType];
    [_mainQueueContext setParentContext:_privateQueueContext];
    _mainQueueContext.undoManager = nil;
}

+ (void)setupInMemoryStoreCoreDataStack {
    // setup our object model and persistent store
    _managedObjectModel = [NSManagedObjectModel mergedModelFromBundles:nil];
    
    NSPersistentStoreCoordinator *coordinator = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:_managedObjectModel];
    
    // create the folder if we need it
    [self createApplicationSupportDirIfNeeded];
    
    // attempt to create the store
    NSError *error = nil;
    _persistentStore = [coordinator addPersistentStoreWithType:NSInMemoryStoreType configuration:nil URL:nil options:[self defaultMigrationOptions] error:&error];
    
    if (!_persistentStore || error) {
        NSLog(@"Unable to create persistent store! %@, %@", error, error.userInfo);
    }
    
    [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(kern_didSaveContext:) name:NSManagedObjectContextDidSaveNotification object:nil];
    
    _privateQueueContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
    [_privateQueueContext setPersistentStoreCoordinator:coordinator];
    
    _mainQueueContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSMainQueueConcurrencyType];
    [_mainQueueContext setParentContext:_privateQueueContext];
}

+ (void)setInitialDataStoreNamed:(NSString *)defaultStore {
    NSFileManager *fileManager = [NSFileManager defaultManager];
    NSURL *destinationURL      = [self urlForStoreName:defaultStore];
    
    if (![fileManager fileExistsAtPath:destinationURL.path]) {
        NSError *error;
        
        NSURL *defaultDataURL = [NSURL fileURLWithPath:[[NSBundle mainBundle] pathForResource:defaultStore ofType:@"sqlite"]];
        
        if (![fileManager copyItemAtURL:defaultDataURL toURL:destinationURL error:&error]) {
            NSLog(@"Failed to use the default SQLite store:\t %@ \nError: %@", defaultStore, error.localizedDescription);
        } else {
            NSLog(@"Successfully copied the default SQLite store:\n%@", defaultStore, destinationURL.path);
            
            // get metadata dictionary for persistentstore and set default database used as initial SQLite store.
            NSMutableDictionary *dictionary = [NSMutableDictionary dictionaryWithDictionary:_persistentStore.metadata.copy];
            [dictionary setObject:@YES forKey:@"DefaultDataImported"];
            [_mainQueueContext.persistentStoreCoordinator setMetadata:dictionary forPersistentStore:_persistentStore];
        }
    }
}

+ (void)cleanUp {
    [[NSNotificationCenter defaultCenter] removeObserver:self name:NSManagedObjectContextDidSaveNotification object:nil];
    _persistentStore     = nil;
    _privateQueueContext = nil;
    _mainQueueContext    = nil;
}

+ (NSDictionary *)defaultMigrationOptions {
    // define the auto migration features
    return [self migrationOptionsUsingSQLiteJournalMode:TRUE];
}

+ (NSDictionary *)migrationOptionsUsingSQLiteJournalMode:(BOOL)useJournal {
    NSString *journalModeChoice = useJournal ? @"WAL" : @"DELETE";
    return @{
               NSMigratePersistentStoresAutomaticallyOption: @YES,
               NSInferMappingModelAutomaticallyOption: @YES,
               NSSQLitePragmasOption: @{@"journal_mode": journalModeChoice}
    };
}

#pragma mark - Core Data Save

+ (void)kern_didSaveContext:(NSNotification *)notification {
    NSManagedObjectContext *mainContext = _mainQueueContext;
    
    if ([notification object] == mainContext) {
        NSManagedObjectContext *parentContext = [mainContext parentContext];
        [parentContext performBlock:^{
            [parentContext save:nil];
        }];
    }
}

+ (BOOL)saveContext {
    NSManagedObjectContext *context = _mainQueueContext;
    
    if (context == nil | ![context hasChanges]) {
        return NO;
    }
    
    NSError *error = nil;
    
    if (![context save:&error]) {
        NSLog(@"Unable to save context! %@, %@", error, error.userInfo);
        return NO;
    }
    
    return YES;
}

#pragma mark - Library Helpers

+ (NSFetchRequest *)kern_fetchRequestForEntityName:(NSString *)entityName condition:(id)condition sort:(id)sort limit:(NSUInteger)limit {
    NSFetchRequest *request = [[NSFetchRequest alloc] initWithEntityName:entityName];
    request.fetchBatchSize = kKernDefaultBatchSize;
    
    if (condition) {
        request.predicate = [self kern_predicateFromConditional:condition];
    }
    
    if (sort) {
        [request setSortDescriptors:[self kern_sortDescriptorsFromObject:sort]];
    }
    
    if (limit > 0) {
        request.fetchLimit = limit;
    }
    
    return request;
}

+ (NSUInteger)kern_countForFetchRequest:(NSFetchRequest *)fetchRequest {
    NSError *error   = nil;
    NSUInteger count = [[self sharedThreadedContext] countForFetchRequest:fetchRequest error:&error];
    
    if (error) {
        [NSException raise:@"Unable to count for fetch request." format:@"Error: %@", error];
    }
    
    return count;
}

+ (NSManagedObjectContext *)sharedThreadedContext {
    if ([NSThread isMainThread]) {
        return _mainQueueContext;
    } else {
        return _privateQueueContext;
    }
}

+ (NSArray *)kern_executeFetchRequest:(NSFetchRequest *)fetchRequest {
    NSError *error   = nil;
    NSArray *results = [[self sharedThreadedContext] executeFetchRequest:fetchRequest error:&error];
    
    if (error) {
        [NSException raise:@"Unable to execute fetch request." format:@"Error: %@", error];
    }
    
    return ([results count] > 0) ? results : nil;
}

#pragma mark - Private

+ (NSArray *)kern_sortDescriptorsFromString:(NSString *)sort {
    if (!sort || [sort isEmpty]) {
        return @[];
    }
    
    NSString *trimmedSort = [sort stringByTrimmingLeadingAndTrailingWhitespaceAndNewlineCharacters];
    
    NSMutableArray *sortDescriptors = [NSMutableArray array];
    NSArray *sortPhrases            = [trimmedSort componentsSeparatedByString:@","];
    
    for (NSString *phrase in sortPhrases) {
        NSArray *parts = [[phrase stringByTrimmingLeadingAndTrailingWhitespaceAndNewlineCharacters] componentsSeparatedByString:@" "];
        
        NSString *sortKey = [(NSString *)[parts firstObject] stringByTrimmingLeadingAndTrailingWhitespaceAndNewlineCharacters];
        
        BOOL sortDescending = false;
        
        if ([parts count] == 2) {
            NSString *sortDirection = [[[parts lastObject] stringByTrimmingLeadingAndTrailingWhitespaceAndNewlineCharacters] uppercaseString];
            
            sortDescending = [sortDirection isEqualToString:@"DESC"];
        }
        
        [sortDescriptors addObject:[NSSortDescriptor sortDescriptorWithKey:sortKey ascending:!sortDescending]];
    }
    
    return sortDescriptors;
}

+ (NSSortDescriptor *)kern_sortDescriptorFromDictionary:(NSDictionary *)dict {
    NSString *value  = [[dict.allValues objectAtIndex:0] uppercaseString];
    NSString *key    = [dict.allKeys objectAtIndex:0];
    BOOL isAscending = ![value isEqualToString:@"DESC"];
    return [NSSortDescriptor sortDescriptorWithKey:key ascending:isAscending];
}

+ (NSSortDescriptor *)kern_sortDescriptorFromObject:(id)order {
    if ([order isKindOfClass:[NSSortDescriptor class]]) {
        return order;
    } else if ([order isKindOfClass:[NSString class]]) {
        return [NSSortDescriptor sortDescriptorWithKey:order ascending:YES];
    } else if ([order isKindOfClass:[NSDictionary class]]) {
        return [self kern_sortDescriptorFromDictionary:order];
    }
    
    return nil;
}

+ (NSArray *)kern_sortDescriptorsFromObject:(id)order {
    // if it's a comma separated string, use our method to parse it
    if ([order isKindOfClass:[NSString class]] && ([order containsString:@","] || [order containsString:@" "])) {
        return [self kern_sortDescriptorsFromString:order];
    } else if ([order isKindOfClass:[NSArray class]]) {
        NSMutableArray *results = [NSMutableArray array];
        
        for (id object in order) {
            [results addObject:[self kern_sortDescriptorFromObject:object]];
        }
        
        return results;
    } else {
        return @[[self kern_sortDescriptorFromObject:order]];
    }
}

+ (NSPredicate *)kern_predicateFromConditional:(id)condition {
    if (condition) {
        if ([condition isKindOfClass:[NSPredicate class]]) { //any kind of predicate?
            return condition;
        } else if ([condition isKindOfClass:[NSString class]]) {
            return [NSPredicate predicateWithFormat:condition];
        } else if ([condition isKindOfClass:[NSDictionary class]]) {
            // if it's empty or not provided return nil
            if (!condition || [condition count] == 0) {
                return nil;
            }
            
            NSMutableArray *subpredicates = [NSMutableArray array];
            
            for (id key in [condition allKeys]) {
                id value = [condition valueForKey:key];
                [subpredicates addObject:[NSPredicate predicateWithFormat:@"%K == %@", key, value]];
            }
            
            return [NSCompoundPredicate andPredicateWithSubpredicates:subpredicates];
        }
        
        @throw [NSException exceptionWithName:@"InvalidConditional" reason:@"Invalid conditional." userInfo:@{}];
    }
    
    return nil;
}

#pragma mark - Testing -

+ (void)drop {
    [self dropDatabase:[self baseName]];
}

+ (void)dropDatabase:(NSString *)sqliteName {
    NSFileManager *fileManager = [NSFileManager defaultManager];
    
    NSURL *baseURL = [[self applicationDocumentsDirectory] URLByAppendingPathComponent:sqliteName];
    NSURL *shmURL  = [baseURL URLByAppendingPathExtension:@"sqlite-shm"];
    NSURL *walURL  = [baseURL URLByAppendingPathExtension:@"sqlite-wal"];
    
    [fileManager removeItemAtURL:[self urlForStoreName:sqliteName] error:nil];
    [fileManager removeItemAtURL:shmURL error:nil];
    [fileManager removeItemAtURL:walURL error:nil];
}

@end
