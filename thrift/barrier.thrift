namespace cpp novalincs.cs.lsd

service barrier {
    bool create (1: string id, 2: i32 expected);
    bool wait   (1: string id                 );
    bool destroy(1: string id                 );
}
