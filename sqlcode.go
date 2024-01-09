package godeltastream

type SqlState string

const (
	SqlState00000  SqlState = "00000"
	SqlState01000  SqlState = "01000"
	SqlState01004  SqlState = "01004"
	SqlState01006  SqlState = "01006"
	SqlState01007  SqlState = "01007"
	SqlState01P01  SqlState = "01P01"
	SqlState02000  SqlState = "02000"
	SqlState03000  SqlState = "03000"
	SqlState0A000  SqlState = "0A000"
	SqlState0L000  SqlState = "0L000"
	SqlState0LP01  SqlState = "0LP01"
	SqlState2BP01  SqlState = "2BP01"
	SqlState3D000  SqlState = "3D000"
	SqlState3F000  SqlState = "3F000"
	SqlState3G000  SqlState = "3G000"
	SqlState42501  SqlState = "42501"
	SqlState42601  SqlState = "42601"
	SqlState42622  SqlState = "42622"
	SqlState42710  SqlState = "42710"
	SqlState42P04  SqlState = "42P04"
	SqlState42P06  SqlState = "42P06"
	SqlState42P001 SqlState = "42P001"
	SqlStateXX000  SqlState = "XX000"
	SqlStateXX001  SqlState = "XX001"
	SqlState200003 SqlState = "200003"
)

const (
	SqlStateSuccessfulCompletion       = SqlState00000
	SqlStateWarning                    = SqlState01000
	SqlStatePrivilegeNotGranted        = SqlState01007
	SqlStatePrivilegeNotRevoked        = SqlState01006
	SqlStateStringDataRightTruncation  = SqlState01004
	SqlStateDeprecatedFeature          = SqlState01P01
	SqlStateNoData                     = SqlState02000
	SqlStateSqlStatementNotYetComplete = SqlState03000
	SqlStateFeatureNotSupported        = SqlState0A000
	SqlStateInvalidGrantor             = SqlState0L000
	SqlStateInvalidGrantOperation      = SqlState0LP01
	SqlStateDependentObjectsStillExist = SqlState2BP01
	SqlStateInvalidDatabaseName        = SqlState3D000
	SqlStateInvalidSchemaName          = SqlState3F000
	SqlStateInvalidOrganizationName    = SqlState3G000
	SqlStateInsufficientPrivilege      = SqlState42501
	SqlStateSyntaxError                = SqlState42601
	SqlStateNameTooLong                = SqlState42622
	SqlStateDuplicateObject            = SqlState42710
	SqlStateDuplicateDatabase          = SqlState42P04
	SqlStateDuplicateSchema            = SqlState42P06
	SqlStateAmbiguousOrganization      = SqlState42P001
	SqlStateInternalError              = SqlStateXX000
	SqlStateUndefined                  = SqlStateXX001
	SqlStateCancelled                  = SqlState200003
)
