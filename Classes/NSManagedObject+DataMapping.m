//
//  NSManagedObject+DataMapping.m
//  Kern
//
//  Created by Dustin Steele on 12/30/13.
//  Copyright (c) 2013 Varsity Tutors. All rights reserved.
//

#import "NSManagedObject+Finders.h"
#import "NSManagedObject+Modifiers.h"
#import "NSManagedObject+DataMapping.h"

static NSDateFormatter *sCachedDateFormatter;
static NSDateFormatter *sCachedTimeFormatter;
static NSMutableDictionary *sKernPrimaryKeyStore;

NSString * const KernIsPrimaryKey = @"__KernIsPrimaryKey";
NSString * const KernDataTypeString = @"__KernDataTypeString";
NSString * const KernDataTypeNumber = @"__KernDataTypeNumber";
NSString * const KernDataTypeBoolean = @"__KernDataTypeBoolean";
NSString * const KernDataTypeDate = @"__KernDataTypeDate";
NSString * const KernDataTypeTime = @"__KernDataTypeTime";
NSString * const KernDataTypeRelationshipBlock = @"__KernDataTypeRelationshipBlock";

NSString * const KernPrimaryKeyAttribute = @"__KernPrimaryKeyAttribute";
NSString * const KernPrimaryKeyRemoteKey = @"__KernPrimaryKeyRemoteKey";

NSUInteger kKernArrayIndexRemoteKey = 0;
NSUInteger kKernArrayIndexDataType = 1;
NSUInteger kKernArrayIndexPrimaryKeyIndicator = 2;
NSUInteger kKernArrayIndexRelationshipBlock = 2;

@implementation NSManagedObject (DataMapping)

+ (NSDictionary*)kern_mappedAttributes {
    return nil;
}

+(NSDateFormatter*)cachedDateFormatter {
    if (sCachedDateFormatter == nil) {
        sCachedDateFormatter = [[NSDateFormatter alloc] init];
        [sCachedDateFormatter setDateFormat:@"yyyy-MM-dd"];
    }
    return sCachedDateFormatter;
}

+(NSDateFormatter*)cachedTimeFormatter {
    if (sCachedTimeFormatter == nil) {
        sCachedTimeFormatter = [[NSDateFormatter alloc] init];
        [sCachedTimeFormatter setDateFormat:@"yyyy-MM-dd'T'HH:mm:ss'Z'"];
    }
    return sCachedTimeFormatter;
}

+ (NSMutableDictionary*)kern_primaryKeyStore {
    
    if (sKernPrimaryKeyStore == nil) {
        sKernPrimaryKeyStore = @{}.mutableCopy;
        
        // create a dictionary if there's not one for this class yet
        if (sKernPrimaryKeyStore[self.class] == nil) {
            
            // get down to just the attributes
            NSDictionary *mappedAttributes = [[[self kern_mappedAttributes] allValues] lastObject];
            for (NSString *k in mappedAttributes) {
                NSArray *obj = [mappedAttributes objectForKey:k];
                
                if ([obj count] > 2 && [[obj objectAtIndex:kKernArrayIndexPrimaryKeyIndicator] isEqualToString:KernIsPrimaryKey]) {
                    NSString *attributeName = [[mappedAttributes allKeysForObject:obj] lastObject];
                    NSString *attributeKey = [obj objectAtIndex:kKernArrayIndexRemoteKey];
                    
                    [sKernPrimaryKeyStore setObject:@{KernPrimaryKeyAttribute: attributeName, KernPrimaryKeyRemoteKey: attributeKey} forKey:[self kern_entityName]];
                }
            }
            
        }
    }
    
    return sKernPrimaryKeyStore;
}

+ (NSString*)kern_primaryKeyAttribute {
    
    return [self kern_primaryKeyStore][self.kern_entityName][KernPrimaryKeyAttribute];
}

+ (NSString*)kern_primaryKeyRemoteKey {
    return [self kern_primaryKeyStore][self.kern_entityName][KernPrimaryKeyRemoteKey];
}

+ (instancetype)findByPrimaryKey:(id)aPrimaryKeyValue {
    
    NSString *pk = [self kern_primaryKeyAttribute];
    if (pk) {
        return [[self findAllWithLimit:1 where:@"%K == %@", [self kern_primaryKeyAttribute], aPrimaryKeyValue] lastObject];
    }
    else {
        @throw [NSException exceptionWithName:@"Cannot find record" reason:@"No primary key defined" userInfo:nil];
    }
}

+ (instancetype)findOrCreateByPrimaryKey:(id)aPrimaryKeyValue {
    id obj = [self findByPrimaryKey:aPrimaryKeyValue];
    if (!obj) {
        obj = [self createEntity];
        
        [obj setValue:aPrimaryKeyValue forKey:[self kern_primaryKeyAttribute]];
    }
    return obj;
}

+ (instancetype)updateOrCreateEntityUsingRemoteDictionary:(NSDictionary *)aDictionary {
    
    NSDictionary *objAttributes = [[aDictionary allValues] lastObject];
    
    id pkValue = [objAttributes valueForKey:[self kern_primaryKeyRemoteKey]];

    // need to have a primary key to create or update
    if (![[objAttributes allKeys] containsObject:[self kern_primaryKeyRemoteKey]]) {
        @throw [NSException exceptionWithName:@"Can't locate primary key" reason:@"Primary key not provided in remote dictionary" userInfo:nil];
    }

    NSManagedObject *obj = [self findOrCreateByPrimaryKey:pkValue];
    
    NSDictionary *mappedAttributes = [[[self.class kern_mappedAttributes] allValues] lastObject];
    NSMutableDictionary *convertedAttributes = [NSMutableDictionary dictionary];

    for (NSString *attributeName in [mappedAttributes allKeys]) {
        NSArray *item = [mappedAttributes objectForKey:attributeName];
        NSString *remoteKey = [item objectAtIndex:kKernArrayIndexRemoteKey];
        // only process key if it's in our provided set
        if ([[objAttributes allKeys] containsObject:remoteKey]) {
            NSString *dataType = [item objectAtIndex:kKernArrayIndexDataType];
            
            if (dataType == KernDataTypeRelationshipBlock) {
                KernCoreDataRelationshipBlock blk = (KernCoreDataRelationshipBlock)[item objectAtIndex:kKernArrayIndexRelationshipBlock];
                blk(self,aDictionary,attributeName,remoteKey);
            }
            else {
                id aValue = [objAttributes valueForKey:remoteKey];

                if (aValue != nil && aValue != [NSNull null]) {
                    if (dataType == KernDataTypeString || dataType == KernDataTypeNumber || dataType == KernDataTypeBoolean) { //strings and numbers (booleans)
                        convertedAttributes[attributeName] = aValue;
                    }
                    else if (dataType == KernDataTypeDate) {
                        NSDate *dateValue = [[self.class cachedDateFormatter] dateFromString:aValue];
                        if (dateValue && ![dateValue isKindOfClass:[NSNull class]]) {
                            convertedAttributes[attributeName] = dateValue;
                        }
                    }
                    else if (dataType == KernDataTypeTime) {
                        NSDate *dateValue = [[self.class cachedTimeFormatter] dateFromString:aValue];
                        if (dateValue && ![dateValue isKindOfClass:[NSNull class]]) {
                            convertedAttributes[attributeName] = dateValue;
                        }
                    }
                    
                }
            }
        }
    }
    
    // set using converted attributes
    [obj updateEntity:convertedAttributes];
    
    return obj;
}

+ (NSUInteger)updateOrCreateEntitiesUsingRemoteArray:(NSArray*)anArray {
    NSUInteger count = 0;
    for (NSDictionary *aDictionary in anArray) {
        [self updateOrCreateEntityUsingRemoteDictionary:aDictionary];
        count++;
    }
    return count;
}

@end