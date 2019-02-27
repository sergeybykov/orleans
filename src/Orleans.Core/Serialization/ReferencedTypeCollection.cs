#if SERIALIZER_SESSIONAWARE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Orleans.Serialization
{
    internal sealed class ReferencedTypeCollection
    {
        private readonly struct ReferencePair
        {
            public ReferencePair(uint id, Type type)
            {
                this.Id = id;
                this.Type = type;
            }

            public uint Id { get; }

            public Type Type { get; }
        }

        private int referenceToTypeCount;
        private ReferencePair[] referenceToType = new ReferencePair[64];

        private int typeToReferenceCount;
        private ReferencePair[] typeToReference = new ReferencePair[64];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetReferencedType(uint reference, out Type value)
        {
            // Reference 0 is always null.
            if (reference == 0)
            {
                value = null;
                return true;
            }

            for (int i = 0; i < this.referenceToTypeCount; ++i)
            {
                if (this.referenceToType[i].Id == reference)
                {
                    value = this.referenceToType[i].Type;
                    return true;
                }
            }

            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MarkValueField() => ++this.CurrentReferenceId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetReference(Type value, out uint reference)
        {
            // TODO: Binary search
            for (int i = 0; i < this.typeToReferenceCount; ++i)
            {
                if (value.Equals(this.typeToReference[i].Type))
                {
                    reference = this.typeToReference[i].Id;
                    return true;
                }
            }

            reference = 0;
            return false;
        }

        private void AddToReferenceToIdMap(Type value, uint reference)
        {
            if (this.typeToReferenceCount >= this.typeToReference.Length)
            {
                var old = this.typeToReference;
                this.typeToReference = new ReferencePair[this.typeToReference.Length * 2];
                Array.Copy(old, this.typeToReference, this.typeToReferenceCount);
            }

            this.typeToReference[this.typeToReferenceCount++] = new ReferencePair(reference, value);
        }

        private void AddToReferences(Type value, uint reference)
        {
            if (this.referenceToTypeCount >= this.referenceToType.Length)
            {
                var old = this.referenceToType;
                this.referenceToType = new ReferencePair[this.referenceToType.Length * 2];
                Array.Copy(old, this.referenceToType, this.referenceToTypeCount);
            }

            this.referenceToType[this.referenceToTypeCount++] = new ReferencePair(reference, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordTypeWhileSerializing(Type value) => this.AddToReferenceToIdMap(value, ++this.CurrentReferenceId);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordTypeWhileDeserializing(Type value) => this.AddToReferences(value, ++this.CurrentReferenceId);

        public Dictionary<uint, Type> CopyReferenceTable() => this.referenceToType.Take(this.referenceToTypeCount).ToDictionary(r => r.Id, r => r.Type);
        public Dictionary<Type, uint> CopyIdTable() => this.typeToReference.Take(this.typeToReferenceCount).ToDictionary(r => r.Type, r => r.Id);

        public uint CurrentReferenceId { get; set; }

        public void Reset()
        {
            for (var i = 0; i < this.referenceToTypeCount; i++)
            {
                this.referenceToType[i] = default;
            }
            for (var i = 0; i < this.typeToReferenceCount; i++)
            {
                this.typeToReference[i] = default;
            }
            this.referenceToTypeCount = 0;
            this.typeToReferenceCount = 0;
            this.CurrentReferenceId = 0;
        }
    }
}
#endif