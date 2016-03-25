All errs must be returned to the caller via gloabl_error::add_error(...).  
Problem: dtors can call add_error(), but there's no way for them to communicate faliures since dtors return nothing.  
Each subsystem has its own set of err numbers (enums bufErrCodes{HASHTBLERR, HASHNOTFOUND}, ...), independent of the other subsystems.  
Corresponding to the set of err numbers, each subsystem also declare an array of error messages, and must make these messages available to the global err obj.  
How can the subsystem make these err msgs available to the global err obj? By creating a static "error_string_table" obj. The ctor of the this obj registers the err msgs with the subsystem when the program first starts.  

Examples three macros for posting errors  
to add a "first" err:  
MINIBASE_FIRST_ERROR(BUFMGR, BUFFEREXCEEDED)  
to add a "chained" err:  
status = MINIBASE_DB->write_page(...);  
if ( status != OK )  
	return MINIBASE_CHAIN_ERROR(BUFMGR, status);  
if you want to post a different msg but still acknowledge that the err resulted from a prior err:  
status = MINIBASE_DB->write_page( ... );  
if ( status != OK )  
	return MINIBASE_RESULTING_ERROR(BUFMGR, status,   BUFFEREXCEEDED);  

Handling errs:  
minibase_errors.show_errors();  
if ( minibase_errors.error() ) ...;  
if ( minibase_errors.originator() == BUFMGR ) ...;  
if ( minibase_errors.error_index() == BUFFEREXCEEDED ) ...;  





